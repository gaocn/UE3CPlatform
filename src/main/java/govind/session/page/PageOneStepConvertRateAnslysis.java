package govind.session.page;

import com.alibaba.fastjson.JSONObject;
import govind.constant.Constants;
import govind.dao.IPageConvertRateDAO;
import govind.dao.ITaskDAO;
import govind.dao.impl.DAOFactory;
import govind.domain.PageConvertRate;
import govind.domain.Task;
import govind.util.DateUtils;
import govind.util.NumberUtils;
import govind.util.ParamUtils;
import govind.util.SparkUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.*;

/**
 * 页面单跳转化率Spark作业
 */
@Slf4j
public class PageOneStepConvertRateAnslysis {
	public static void main(String[] args) {
		//1. 构造上下文
		SparkConf conf = new SparkConf()
				.setAppName(Constants.SPARK_APP_NAME_PAGE);
		SparkUtils.setMaster(conf);

		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = SparkUtils.getSQLContext(sc);

		//2. 生成模拟数据
		SparkUtils.mockData(sc, sqlContext);

		//3. 查询任务，获取任务参数
		Long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PAGE);
		if (taskid == null) {
			log.info("{}: 找不到任务id[{}]", new Date(), taskid);
			return;
		}

		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		Task task = taskDAO.findById(taskid);
		/* 解析任务参数为JSON对象 */
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

		//4. 查询指定日期范围内的用户访问行为数据
		JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);

		/**
		 * 对用户访问行为数据做映射，将其映射为<sessionid, 访问行为>的格式，
		 * 用户访问页面切片的生成是要基于每个session的访问数据来进行生成的
		 * 脱离了session，生成的页面访问切片是没有意义的。
		 *
		 * 假设用户指定的页面流筛选条件：页面3-->页面4-->页面7，注意这里是不
		 * 能直接页面3-->页面4串起来的作为一个页面切片统计吗？不行，页面切片
		 * 的生成是要基于用户session粒度的。
		 */
		JavaPairRDD<String, Iterable<Row>> sessionid2actionRDD = getSessionid2actionRDD(actionRDD).groupByKey();
		//重复使用需要持久化
		sessionid2actionRDD = sessionid2actionRDD.cache();

		//5. 每个session的单跳页面切片的生成及页面流的匹配算法
		JavaPairRDD<String, Integer> pageSplitRDD = generateAndMatchPageSplit(sc, sessionid2actionRDD, taskParam);
		/**
		 * 用户自行页面流为3,2,5,8,6
		 * 则计算结果为：3->2的pv，2->5的pv，5->8的pv，8->6的pv
		 * 上述结果只能2,5,8,6页面的转化率，而不能计算页面3的转换率
		 */
		Map<String, Object> pageSplitPVMap = pageSplitRDD.countByKey();
		//为了计算页面3的转化率，需要起始页面3的访问量
		long startPagePV = getStartPagePV(taskParam, sessionid2actionRDD);
		System.out.println(pageSplitPVMap);
		System.out.println(startPagePV);
		//6. 计算各个页面切片的转化率
		// 返回Map中的格式为：<"2_3", 29>
		Map<String, Double> convertRateMap = computeConvertRate(taskParam, pageSplitPVMap, startPagePV);

		//7. 存储页面转化率到MySQL数据库中
		//页面流中各个页面切片的转化率，以特定格式拼接起来，例如3,5=10%|5,7=20%
		persistConvertRate(taskid, convertRateMap);
	}

	private static Map<String, Double> computeConvertRate(JSONObject taskParam, Map<String, Object> pageSplitPVMap, long startPagePV) {
		Map<String, Double> convertRateMap = new HashMap<>();
		String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
		String[] targetPages = targetPageFlow.split(",");
		long lastPageSplitPV = 0L;
		for (int i = 1; i < targetPages.length; i++) {
			String targetPageSplit = targetPages[i-1] + "_" + targetPages[i];
			long targetPageSplitPV = (long) pageSplitPVMap.get(targetPageSplit);

			double convertRate;
			//首页
			if (i == 1) {
				convertRate = NumberUtils.formatDouble(targetPageSplitPV / (double)startPagePV, 2);
			} else {
				convertRate = NumberUtils.formatDouble(targetPageSplitPV / (double)lastPageSplitPV, 2);
			}
			convertRateMap.put(targetPageSplit, convertRate);
			lastPageSplitPV = targetPageSplitPV;
		}
		return convertRateMap;
	}

	/**
	 * 获取<sessionid, 用户访问行为>格式的数据
	 */
	private static JavaPairRDD<String, Row> getSessionid2actionRDD(JavaRDD<Row> actionRDD) {
		return actionRDD.mapToPair((PairFunction<Row, String, Row>) row -> {
			String sessionId = row.getString(2);
			return new Tuple2<>(sessionId, row);
		});
	}

	/**
	 * 页面切片生成与匹配算法
	 * @return 元组格式 <"2_3", 1>
	 */
	private static JavaPairRDD<String, Integer> generateAndMatchPageSplit(JavaSparkContext sc, JavaPairRDD<String, Iterable<Row>> sessionid2actionRDD, JSONObject taskParam) {
		String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
		final Broadcast<String> targetPageFlowBroadcast = sc.broadcast(targetPageFlow);

		return sessionid2actionRDD.flatMapToPair((PairFlatMapFunction<Tuple2<String, Iterable<Row>>, String, Integer>) tuple2 -> {
			List<Tuple2<String, Integer>> result = new ArrayList<>();
			Iterator<Row> iter = tuple2._2.iterator();
			//3,5,7,9 -> [3,5,7,9]
			String[] targetPages = targetPageFlowBroadcast.value().split(",");

			/**
			 * session访问行为默认是乱序的，一般我们希望拿到的数据是按照时
			 * 间排序的，因此需要对session访问行为数据按照时间排序。
			 * 例如：乱序页面流3->5->4->10->7
			 * 		排序后的流3->4->5->7->10
			 */
			List<Row> orderedRows = new ArrayList<>();
			while (iter.hasNext()) {
				orderedRows.add(iter.next());
			}
			//按照时间排序的同一用户访问session
			Collections.sort(orderedRows, new Comparator<Row>() {
				@Override
				public int compare(Row o1, Row o2) {
					String actionTime1 = o1.getString(4);
					String actionTime2 = o2.getString(4);
					Date date1 = DateUtils.parseTime(actionTime1);
					Date date2 = DateUtils.parseTime(actionTime2);
					return (int) (date1.getTime() - date2.getTime());
				}
			});

			//页面切片生及页面流匹配
			Long lastPageId = null;
			for (Row row : orderedRows) {
				long pageid = row.getLong(3);
				if (lastPageId == null) {
					lastPageId = pageid;
					continue;
				}
				//生成页面切片
				//lastPageId=3, pageid=2 => 3_2
				String pageSplit = lastPageId + "_" + pageid;

				//判断当前切片是否在用户指定的页面流中
				//例如：用户页面流3,2,5,8,1  pageSplit = 3_2
				for (int i = 1; i < targetPages.length; i++) {
					String targetPageSplit = targetPages[i - 1] + "_" + targetPages[i];
					if (targetPageSplit.equals(pageSplit)) {
						result.add(new Tuple2<>(pageSplit, 1));
						break;
					}
				}
				lastPageId = pageid;
			}
			return result;
		});
	}

	/**
	 * 获取页面流中初始页面的PV
	 * @return
	 */
	private static long getStartPagePV(JSONObject taskParam, JavaPairRDD<String, Iterable<Row>> sessionid2actionRDD) {
		String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
		final long startPageId = Long.valueOf(targetPageFlow.split(",")[0]);
		JavaRDD<Long> startPagePV = sessionid2actionRDD.flatMap((FlatMapFunction<Tuple2<String, Iterable<Row>>, Long>) tuple2 -> {
			List<Long> list = new ArrayList<>();
			Iterator<Row> iter = tuple2._2.iterator();

			while (iter.hasNext()) {
				Row row = iter.next();
				long pageid = row.getLong(3);
				if (pageid == startPageId) {
					list.add(pageid);
				}
			}
			return list;
		});
		return startPagePV.count();
	}

	/**
	 * 持久化页面转化率到MySQL数据库
	 */
	private static void persistConvertRate(Long taskid, Map<String, Double> convertRateMap) {
		StringBuffer buffer = new StringBuffer();
		for (Map.Entry<String, Double> entry : convertRateMap.entrySet()) {
			buffer.append(entry.getKey()).append("=" )
					.append( entry.getValue()).append("|");
		}
		String convertRate = buffer.substring(0, buffer.length() - 1);

		IPageConvertRateDAO pageConvertRateDAO = DAOFactory.getPageConvertRateDAO();
		PageConvertRate pageConvertRate = new PageConvertRate();
		pageConvertRate.setTaskid(taskid);
		pageConvertRate.setConvertRate(convertRate);
		pageConvertRateDAO.insert(pageConvertRate);
	}
}

package govind.session;

import govind.conf.ConfigurationManager;
import govind.constant.Constants;
import govind.dao.*;
import govind.dao.impl.DAOFactory;
import govind.domain.*;
import govind.mock.MockData;
import govind.util.*;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.ints.IntList;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.Rand;
import org.apache.spark.sql.hive.HiveContext;
import com.alibaba.fastjson.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

/**
 * 用户访问session分析作业
 */
@Slf4j
public class UserVisitSessionAnalysis {
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "E:\\GIT\\spark\\hadoop-2.6.5");
		//1、构建Spark上下文
		SparkConf conf = new SparkConf()
				.setAppName(Constants.SPARK_APP_NAME_SESSION)
				.set("spark.serializer", "org.apache.spark.serializer.KyroSerializer")
				.set("spark.shuffle.io.maxRetries", "60")
				.set("spark.shuffle.io.retryWait", "60")
				.registerKryoClasses(new Class[]{
						//获取Top10热门品类时的自定义二次排序，在shuffle时需要网络传输，
						// 因此启用Kyro序列化机制后，为了优化性能需要进行注册
						CategorySortKey.class,
						IntList.class
				});
		SparkUtils.setMaster(conf);

		//2、构建上下文
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = SparkUtils.getSQLContext(sc);

		//3、生成模拟数据
		SparkUtils.mockData(sc, sqlContext);

		/**
		 * 若要根据用户在创建任务时指定的参数来进行数据过滤和筛选，就得查询
		 * 指定的任务。
		 */
		//获取要执行任务的id
		Long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION);
		//根据taskid获取任务信息及任务参数
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		Task task = taskDAO.findById(taskid);
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

		/**
		 * 如果要进行session粒度的数据聚合，需要从user_visit_actio表
		 * 中查询指定日期范围内的行为数据。
		 *
		 * actionRDD是一个公共RDD：
		 * 1、要用actionRDD获取到一个公共的sessionid为Key的PairRDD
		 * 2、在session聚合环节里面也用到的actionRDD
		 *
		 */
		JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);
		JavaPairRDD<String, Row> sessionId2ActionRDD = getSessionid2ActionRdd(actionRDD);
		//被使用多次，因此持久化，默认存放内存
		sessionId2ActionRDD = sessionId2ActionRDD.persist(StorageLevel.MEMORY_ONLY());
		/**
		 * 将行为数据根据session_id进行groupBy分组，此时的粒度就是session
		 * 粒度，然后将session粒度数据与用户信息JOIN就可以获取session+user
		 * 粒度的信息，才能进行后续的过滤
		 */
		JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySessionId(sc, sessionId2ActionRDD, sqlContext);

		//System.out.println(sessionid2AggrInfoRDD.count());
		//sessionid2AggrInfoRDD.take(10).forEach(System.out::println);

		//重构，同时进行过滤和统计
		Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("", new SessionAggrAccumulator());

		/**
		 * 针对session粒度的聚合数据，按照使用者指定的筛选参数进行过滤
		 */
		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);
		//被使用了两次，需要持久化
		filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());

		System.out.println(filteredSessionid2AggrInfoRDD.count());
		filteredSessionid2AggrInfoRDD.take(10).forEach(System.out::println);

		/**
		 * session聚合统计
		 * 统计访问时长、访问步长各个区间的session数量占总session的比例
		 * 如果不进行重构，直接实现的思路：
		 * 1、actionRDD映射为<sessionid, Row>格式
		 * 2、按session聚合计算出每个session的访问时长和访问步长，生成RDD
		 * 3、遍历新的RDD根据每个session的访问时长和步长更新自定义Accumulator
		 * 4、使用自定义Accumulator中的统计值计算各个区间的比例；
		 * 5、将最后计算结果写入MySQL
		 *
		 * 上述思路的问题：1、为什么还用actionRDD，之前对session聚合时映射
		 * 已经做过；2、是不是一定要为了session聚合功能单独遍历一遍session
		 * 是没有必要的。之前在过滤时已经遍历了session，这里没哟必要再次遍历。
		 *
		 * 重构：1、不要去生成任何新的RDD(意味着要处理三亿条数据)；2、不要单
		 * 独遍历仪表session数据(可能要处理千万条数据)。3、可以在session聚
		 * 合时就计算出session的访问时长和访问步长。4、在进行过滤时本来就需
		 * 要遍历所有聚合的session信息，此时可以在某个session通过筛选条件
		 * 后将其访问时长和访问步长累加到自定义Accumulator中。
		 *
		 * 注意：上述两种不同思想方式，在面对上亿数据时有时运行时间会差别在长
		 * 达半小时或数小时，即生成新RDD比较浪费时间。
		 *
		 * 开发大型复杂项目的经验准则：1、尽量少生成RDD；2、计量少对RDD进行
		 * 算子操作，若有可能尽量在一个算子里面实现要做的功能。3、尽量少对RDD
		 * 进行shuffle算子操作，比如groupByKey、reduceByKey、sortByKey
		 * shuffle操作，会导致大量磁盘读写影响性能，有shuffle算子的操作可能
		 * 会有几十分钟到数小时的计算延迟。
		 */

		/**
		 * 特别说明，要将上一个功能的session聚合统计数据拿到，就必须是在一个
		 * action操作触发job之后才能从Accumulator中获取数据，否则是获取不
		 * 到数据的，因为没有Job执行，Accumulator的值为空，所以这里将随机抽
		 * 取的功能实现放在session聚合统计功能的最终计算和写库之前，因为随机
		 * 抽取功能中，有一个countByKey算子是action操作会触发Job。
		 */

		randomExtractSession(sc, filteredSessionid2AggrInfoRDD, taskid, sessionId2ActionRDD);


		/**
		 * 计算出各个范围的session占比并写入MySQL数据库
		 * 要保证下面结果保存在数据库，需要Action触发作业才能运行，但是Action
		 * 操作不能在该函数之后，否则计算结果为空，必须在该函数之前调用Action
		 * 触发作业，把数据计算完成后再写入数据库！！！
		 */
		calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), taskid);

		/**
		 * Top10热门品类
		 */
		//重构 JavaPairRDD<String, Row> sessionid2ActionRdd = getSessionid2ActionRdd(actionRDD);
		//重构
		JavaPairRDD<String, Row> sessionid2DetailRDD = getSession2DetailRDD(filteredSessionid2AggrInfoRDD, sessionId2ActionRDD);
		sessionid2DetailRDD.persist(StorageLevel.MEMORY_ONLY());
		List<Tuple2<CategorySortKey, String>> top10CategoryList = getTop10Category(sessionid2DetailRDD, taskid);

		/**
		 * Top10活跃用户
		 */
		getTop10Session(sc, taskid, sessionid2DetailRDD, top10CategoryList);

		sc.close();
	}

	/**
	 * 对行为数据按照session粒度进行聚合
	 *
	 * @param sessionId2ActionRDD 行为数据，一条数据为一次访问记录：一次搜索、下单或点击
	 * @return Key为user_id方便与用户信息表JOIN，value为拼接的数据
	 */
	private static JavaPairRDD<String, String> aggregateBySessionId(JavaSparkContext sc, JavaPairRDD<String, Row> sessionId2ActionRDD, SQLContext sqlContext) {
		//fix 重构
//		JavaPairRDD<String, Row> sessionId2ActionRDD = actionRDD.mapToPair((PairFunction<Row, String, Row>) row -> {
//			//第一个参数为函数输入，第二个和第三个为输出Tuple
//			return new Tuple2<>(row.get(2).toString(), row);
//		});
		//对行为数据按照session粒度分组
		JavaPairRDD<String, Iterable<Row>> groupBySessionId = sessionId2ActionRDD.groupByKey();
		//对分组后session，聚合session中所有搜索词、点击品类
		JavaPairRDD<Long, String> userId2PartAggrInfoRDD = groupBySessionId.mapToPair((PairFunction<Tuple2<String, Iterable<Row>>, Long, String>) tuple -> {
			String sessionId = tuple._1;
			Iterator<Row> iter = tuple._2.iterator();

			StringBuffer searchKeywordsBuf = new StringBuffer();
			StringBuffer clickCategoryIdsBuf = new StringBuffer();

			Long userId = null;

			//session访问时长
			Date startTime = null;
			Date endTime = null;
			//session访问步长
			int stepLength = 0;

			//遍历session所有的访问行为，提取每个访问行为的搜索词字段和点击品类字段
			while (iter.hasNext()) {
				Row row = iter.next();
				if (userId == null) {
					userId = row.getLong(1);
				}
				String searchKeyword = null;
				Long clickCategoryId = null;
				try {
					searchKeyword = row.getString(5);
					clickCategoryId = row.getLong(6);
				} catch (Exception e) {
					//若取值为空则跳过
				}
				//并不是每一行都有上述两个字段，只有搜索行为有搜索词，而只
				// 有点击行为有点击品类字段，所以数据可能为null！字段不能
				// 为空且StringBuffer中没有改值时才能拼接进去
				if (StringUtils.isNonEmpty(searchKeyword)) {
					if (!searchKeywordsBuf.toString().contains(searchKeyword)) {
						searchKeywordsBuf.append(searchKeyword).append(",");
					}
				}
				if (clickCategoryId != null) {
					if (!clickCategoryIdsBuf.toString().contains(String.valueOf(clickCategoryId))) {
						clickCategoryIdsBuf.append(clickCategoryId).append(",");
					}
				}

				//计算session开始和结束时间
				Date actionTime = DateUtils.parseTime(row.getString(4));
				if (startTime == null) {
					startTime = actionTime;
				}
				if (endTime == null) {
					endTime = actionTime;
				}
				if (actionTime.before(startTime)) {
					startTime = actionTime;
				}
				if (actionTime.after(endTime)) {
					endTime = actionTime;
				}
				//计算session访问步长
				stepLength++;
			}
			String searchKeywords = StringUtils.trimComma(searchKeywordsBuf.toString());
			String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuf.toString());
			//计算session访问时长，单位秒
			long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;

			//返回数据需要与用户信息表进行JOIN，因此返回的元组需要有user_id
			// 才能与<user_id, Row>进行JOIN，将JOIN后的数据的元组的Key
			// 再设置为session_id。聚合数据采用key1=value1|key2=value2..
			// 方式拼接数据
			String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
					+ Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
					+ Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
					+ Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime) + "|"
					+ Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
					+ Constants.FIELD_STEP_LENGTH + "=" + stepLength;
			return new Tuple2<>(userId, partAggrInfo);
		});

		//查询所有用户信息并映射为<userid, Row>的形式
		JavaRDD<Row> userRDD = sqlContext.sql("select * from user_info").javaRDD();
		JavaPairRDD<Long, Row> userid2InfoRDD = userRDD.mapToPair((PairFunction<Row, Long, Row>) row -> {
			return new Tuple2<>(row.getLong(0), row);
		});

		//将session粒度聚合数据与用户信息JOIN
		JavaPairRDD<Long, Tuple2<Row, String>> useridFullInfoRDD = userid2InfoRDD.join(userId2PartAggrInfoRDD);
		/**
		 * 这里就比较适合将reduce join转换为map join
		 * userid2InfoRDD数据量一般很小，例如10万用户
		 * userId2PartAggrInfoRDD数据量可能很大，例如100万条
		 */
//		List<Tuple2<Long, Row>> userInfos = userid2InfoRDD.collect();
//		Broadcast<List<Tuple2<Long, Row>>> userInfosBroadcast = sc.broadcast(userInfos);
//		userId2PartAggrInfoRDD.mapToPair((PairFunction<Tuple2<Long, String>, String, String>) tuple2 -> {
//			//用户信息
//			List<Tuple2<Long, Row>> userInfoList = userInfosBroadcast.value();
//			Map<Long, Row> userInfoMap = new HashMap<>();
//			userInfoList.forEach(userInfo -> userInfoMap.put(userInfo._1, userInfo._2));
//
//			//获取当前用户对应的信息
//			Row userInfoRow = userInfoMap.get(tuple2._1);
//			String partAggrInfo = tuple2._2;
//
//			String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
//			int age = userInfoRow.getInt(3);
//			String professional = userInfoRow.getString(4);
//			String city = userInfoRow.getString(5);
//			String sex = userInfoRow.getString(6);
//
//			String fullAggrInfo = partAggrInfo + "|"
//					+ Constants.FIELD_AGE + "=" + age + "|"
//					+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
//					+ Constants.FIELD_CITY + "=" + city + "|"
//					+ Constants.FIELD_SEX + "=" + sex;
//			return new Tuple2<>(sessionid, fullAggrInfo);
//		});

		//对join起来的数据进行拼接，并返回<sessionid, fullAggeInfo>格式
		JavaPairRDD<String, String> sessionid2FullInfoRDD = useridFullInfoRDD.mapToPair((PairFunction<Tuple2<Long, Tuple2<Row, String>>, String, String>) tuple -> {
			Row userInfoRow = tuple._2._1;
			String partAggrInfo = tuple._2._2;
			String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
			int age = userInfoRow.getInt(3);
			String professional = userInfoRow.getString(4);
			String city = userInfoRow.getString(5);
			String sex = userInfoRow.getString(6);

			String fullAggrInfo = partAggrInfo + "|"
					+ Constants.FIELD_AGE + "=" + age + "|"
					+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
					+ Constants.FIELD_CITY + "=" + city + "|"
					+ Constants.FIELD_SEX + "=" + sex;
			return new Tuple2<>(sessionid, fullAggrInfo);
		});

		/**
		 * sample采样倾斜key单独进行join
		 */
//		JavaPairRDD<Long, String> sampledRDD = userId2PartAggrInfoRDD.sample(false, 0.3, 41);
//		final Long skewedUserId = sampledRDD
//				.mapToPair((PairFunction<Tuple2<Long, String>, Long, Long>) tuple2 -> new Tuple2<>(tuple2._1, 1L))
//				.reduceByKey((Function2<Long, Long, Long>) (v1, v2) -> v1 + v2)
//				.mapToPair((PairFunction<Tuple2<Long, Long>, Long, Long>) tuple2 -> new Tuple2<>(tuple2._2, tuple2._1))
//				.sortByKey(false)
//				.take(1).get(0)._2;
//
//		JavaPairRDD<Long, String> skewedRDD = userId2PartAggrInfoRDD.filter((Function<Tuple2<Long, String>, Boolean>) v1 -> v1._1.equals(skewedUserId));
//
//		JavaPairRDD<Long, String> commonRDD = userId2PartAggrInfoRDD.filter((Function<Tuple2<Long, String>, Boolean>) v1 -> !v1._1.equals(skewedUserId));
//
//		JavaPairRDD<String, Row> skewedUserid2RDD = userid2InfoRDD
//				.filter((Function<Tuple2<Long, Row>, Boolean>) tuple -> tuple._1.equals(skewedUserId))
//				.flatMapToPair((PairFlatMapFunction<Tuple2<Long, Row>, String, Row>) tuple -> {
//					Random rand = new Random();
//					List<Tuple2<String, Row>> list = new ArrayList<>();
//					for (int i = 0; i < 100; i++) {
//						int prefix = rand.nextInt(100);
//						list.add(new Tuple2<>(prefix + "_" + tuple._1, tuple._2));
//					}
//					return list;
//				});
//
//		JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD1 = skewedRDD
//				.mapToPair((PairFunction<Tuple2<Long, String>, String, String>) tuple2 -> {
//					Random rand = new Random();
//					int prefix = rand.nextInt(100);
//					return new Tuple2<String, String>(prefix + "_" + tuple2._1, tuple2._2);
//				})
//				.join(skewedUserid2RDD)
//				.mapToPair((PairFunction<Tuple2<String, Tuple2<String, Row>>, Long, Tuple2<String, Row>>) tuple2 -> {
//					long userid = Long.valueOf(tuple2._1.split("_")[1]);
//					return new Tuple2<>(userid, tuple2._2);
//				});
//
//		JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD2 = commonRDD.join(userid2InfoRDD);
//		JavaPairRDD<Long, Tuple2<String, Row>> unionRDD = joinedRDD1.union(joinedRDD2);
//		unionRDD.mapToPair((PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>) tuple -> {
//			Row userInfoRow = tuple._2._2;
//			String partAggrInfo = tuple._2._1;
//			String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
//			int age = userInfoRow.getInt(3);
//			String professional = userInfoRow.getString(4);
//			String city = userInfoRow.getString(5);
//			String sex = userInfoRow.getString(6);
//
//			String fullAggrInfo = partAggrInfo + "|"
//					+ Constants.FIELD_AGE + "=" + age + "|"
//					+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
//					+ Constants.FIELD_CITY + "=" + city + "|"
//					+ Constants.FIELD_SEX + "=" + sex;
//			return new Tuple2<>(sessionid, fullAggrInfo);
//		});




		return sessionid2FullInfoRDD;
	}

	/**
	 * 过滤session数据
	 */
	public static JavaPairRDD<String, String> filterSessionAndAggrStat(JavaPairRDD<String, String> session2AggrInfoRDD,
																	   final JSONObject taskParam,
																	   Accumulator<String> sessionAggrStatAccumulator) {
		//为了使用ValidateUtils首先将所有参数拼接成一个连接字符串
		String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
		String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
		String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
		String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
		String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
		String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_SEARCH_KEYOWORDS);
		String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

		String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
				+ (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
				+ (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
				+ (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
				+ (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
				+ (keywords != null ? Constants.PARAM_SEARCH_KEYOWORDS + "=" + keywords + "|" : "")
				+ (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds : "");
		if (_parameter.endsWith("\\|")) {
			_parameter = _parameter.substring(0, _parameter.length() - 1);
		}

		final String parameter = _parameter;
		System.out.println("parameter: " + parameter);
		//按照筛选条件进行过滤
		JavaPairRDD<String, String> filteredSessionid2AggrInfo = session2AggrInfoRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, String> v1) throws Exception {
				//获取聚合数据
				String aggrInfo = v1._2;
				//按照年龄范围过滤
				if (!ValidateUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
					return false;
				}
				//按照职业范围过滤，例如：互联网,IT,软件
				if (!ValidateUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)) {
					return false;
				}
				//按照城市范围过滤，例如：北京,上海,广州,深圳
				if (!ValidateUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) {
					return false;
				}
				//按照性别过滤，例如：男,女
				if (!ValidateUtils.equal(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) {
					return false;
				}
				//按照搜索词过滤
				if (!ValidateUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_SEARCH_KEYOWORDS)) {
					return false;
				}
				//按照点击品类ID过滤
				if (!ValidateUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)) {
					return false;
				}

				//若通过了上述过滤条件，说明这条session是需要保留的，接下来就需
				// 要对session的访问时长和访问步长进行统计并累加计数
				sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
				//根据session访问时长和访问步长范围进行累加
				long visitLength = Long.parseLong(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
				long stepLength = Long.parseLong(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
				calculateVisitLength(visitLength);
				calculateStepLength(stepLength);
				return true;
			}

			/**
			 * 计算访问时长范围
			 */
			private void calculateVisitLength(long visitLength) {
				if (visitLength >= 1 && visitLength <= 3) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
				} else if (visitLength >= 4 && visitLength <= 6) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
				} else if (visitLength >= 7 && visitLength <= 9) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
				} else if (visitLength >= 10 && visitLength <= 30) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
				} else if (visitLength >= 30 && visitLength <= 60) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
				} else if (visitLength >= 60 && visitLength <= 180) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
				} else if (visitLength >= 180 && visitLength <= 600) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
				} else if (visitLength >= 600 && visitLength <= 1800) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
				} else if (visitLength >= 1800) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
				}
			}

			/**
			 * 计算访问步长
			 */
			private void calculateStepLength(long stepLength) {
				if (stepLength >= 1 && stepLength <= 3) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
				} else if (stepLength >= 4 && stepLength <= 6) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
				} else if (stepLength >= 7 && stepLength <= 9) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
				} else if (stepLength >= 10 && stepLength <= 30) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
				} else if (stepLength >= 30 && stepLength <= 60) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
				} else if (stepLength >= 60) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
				}
			}
		});
		return filteredSessionid2AggrInfo;
	}

	/**
	 * 计算各session范围占比并写入数据库
	 */
	private static void calculateAndPersistAggrStat(String value, long taskId) {
		System.out.println(value);
		long sessionCount = Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));
		long visitLength1s_3s = Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s));
		long visitLength4s_6s = Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s));
		long visitLength7s_9s = Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s));
		long visitLength10s_30s = Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s));
		long visitLength30s_60s = Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s));
		long visitLength1m_3m = Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m));
		long visitLength3m_10m = Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m));
		long visitLength10m_30m = Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m));
		long visitLength30m = Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m));

		long stepLength1_3 = Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3));
		long stepLength4_6 = Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6));
		long stepLength7_9 = Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9));
		long stepLength10_30 = Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30));
		long stepLength30_60 = Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60));
		long stepLength60 = Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60));

		double visiLength1s3sRation = NumberUtils.formatDouble((double) visitLength1s_3s / (double) sessionCount, 2);
		double visiLength4s6sRation = NumberUtils.formatDouble((double) visitLength4s_6s / (double) sessionCount, 2);
		double visiLength7s9sRation = NumberUtils.formatDouble((double) visitLength7s_9s / (double) sessionCount, 2);
		double visiLength10s30sRation = NumberUtils.formatDouble((double) visitLength10s_30s / (double) sessionCount, 2);
		double visiLength30s60sRation = NumberUtils.formatDouble((double) visitLength30s_60s / (double) sessionCount, 2);
		double visiLength1m3mRation = NumberUtils.formatDouble((double) visitLength1m_3m / (double) sessionCount, 2);
		double visiLength3m10mRation = NumberUtils.formatDouble((double) visitLength3m_10m / (double) sessionCount, 2);
		double visiLength10m30mRation = NumberUtils.formatDouble((double) visitLength10m_30m / (double) sessionCount, 2);
		double visiLength30mRation = NumberUtils.formatDouble((double) visitLength30m / (double) sessionCount, 2);

		double stepLength13Ration = NumberUtils.formatDouble((double) stepLength1_3 / (double) sessionCount, 2);
		double stepLength46Ration = NumberUtils.formatDouble((double) stepLength4_6 / (double) sessionCount, 2);
		double stepLength79Ration = NumberUtils.formatDouble((double) stepLength7_9 / (double) sessionCount, 2);
		double stepLength1030Ration = NumberUtils.formatDouble((double) stepLength10_30 / (double) sessionCount, 2);
		double stepLength3060Ration = NumberUtils.formatDouble((double) stepLength30_60 / (double) sessionCount, 2);
		double stepLength60Ration = NumberUtils.formatDouble((double) stepLength60 / (double) sessionCount, 2);
		SessionAggrStat sessionAggrStat = new SessionAggrStat();
		sessionAggrStat.setTaskid(taskId);
		sessionAggrStat.setSessionCount(sessionCount);
		sessionAggrStat.setVisiLength1s3sRation(visiLength1s3sRation);
		sessionAggrStat.setVisiLength4s6sRation(visiLength4s6sRation);
		sessionAggrStat.setVisiLength7s9sRation(visiLength7s9sRation);
		sessionAggrStat.setVisiLength10s30sRation(visiLength10s30sRation);
		sessionAggrStat.setVisiLength30s60sRation(visiLength30s60sRation);
		sessionAggrStat.setVisiLength1m3mRation(visiLength1m3mRation);
		sessionAggrStat.setVisiLength3m10mRation(visiLength3m10mRation);
		sessionAggrStat.setVisiLength10m30mRation(visiLength10m30mRation);
		sessionAggrStat.setVisiLength30mRation(visiLength30mRation);
		sessionAggrStat.setStepLength13Ration(stepLength13Ration);
		sessionAggrStat.setStepLength46Ration(stepLength46Ration);
		sessionAggrStat.setStepLength79Ration(stepLength79Ration);
		sessionAggrStat.setStepLength1030Ration(stepLength1030Ration);
		sessionAggrStat.setStepLength3060Ration(stepLength3060Ration);
		sessionAggrStat.setStepLength60Ration(stepLength60Ration);

		//调用对应DAO方法插入统计结果
		ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
		sessionAggrStatDAO.insert(sessionAggrStat);
		System.out.println("session聚合统计结果插入完成！");
	}

	/**
	 * 随机抽取session
	 */
	private static void randomExtractSession(JavaSparkContext sc, JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD, long taskid, JavaPairRDD<String, Row> sessionid2ActionRDD) {
		/**
		 * 第一步：计算每天每小时的session数量 映射为<yyyy-MM-dd_HH, aggrInfo>
		 */
		JavaPairRDD<String, String> time2SessionidRDD = filteredSessionid2AggrInfoRDD.mapToPair((PairFunction<Tuple2<String, String>, String, String>) tuple2 -> {
			String aggrInfo = tuple2._2;
			String startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME);
			String dateHour = DateUtils.getDateHour(startTime);
			return new Tuple2<>(dateHour, aggrInfo);
		});
		/**
		 * 第二步：得到每天每小时的session数量
		 */
		Map<String, Object> countMap = time2SessionidRDD.countByKey();

		/**
		 * 第三步：使用按时间比例随机抽取算法，计算出每天每小时要抽取的session索引
		 */
		/*将<yyyy-MM-dd_HH, count>格式的Map转换为<yyyy-MM-dd, <HH, count>>的格式*/
		Map<String, Map<String, Long>> dayHourCountMap = new HashMap<>();
		for (Map.Entry<String, Object> entry : countMap.entrySet()) {
			String dateHour = entry.getKey();
			String date = dateHour.split("_")[0];
			String hour = dateHour.split("_")[1];
			long count = Long.parseLong(String.valueOf(entry.getValue()));

			Map<String, Long> hourCountMap = dayHourCountMap.computeIfAbsent(date, k -> new HashMap<>());
			hourCountMap.put(hour, count);
		}
		//总共要抽取100个session，先按照天进行平分
		int extractNumberPerDay = 100 / dayHourCountMap.size();

		/**
		 * 这里用到了较大的变量，随机抽取索引Map，在算子中实现每个Task都会
		 * 拷贝一份副本，比较消耗内存和网络传输性能。
		 *
		 * 优化方案：将Map做成广播变量
		 */
		//<date, <hour, (3,5,20,102)>>
		Map<String, Map<String, List<Integer>>> dateHourExtractMap = new HashMap<>();
		Random random = new Random();

		for (Map.Entry<String, Map<String, Long>> dateHourCountEntry : dayHourCountMap.entrySet()) {
			String date = dateHourCountEntry.getKey();
			Map<String, Long> hourCountMap = dateHourCountEntry.getValue();

			//计算出一天的session总量
			long sessionCount = 0L;
			for (long hourCount : hourCountMap.values()) {
				sessionCount += hourCount;
			}

			Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.computeIfAbsent(date, k -> new HashMap<>());

			//遍历每小时
			for (Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
				String hour = hourCountEntry.getKey();
				long count = hourCountEntry.getValue();
				//计算每小时的session数量占当天总session数量的比例，乘以
				// 每天要抽取的数量计算出每个小时应该要抽取的量
				int hourExtractNumber = (int) (((double) count / sessionCount) * extractNumberPerDay);

				if (hourExtractNumber > count) {
					hourExtractNumber = (int) count;
				}

				//获取用于存放随机索引的列表
				List<Integer> extractIndexList = hourExtractMap.computeIfAbsent(hour, k -> new ArrayList<>());
				//随机生成数量hourExtractNumber个随机索引
				for (int i = 0; i < hourExtractNumber; i++) {
					int randIndex = random.nextInt((int) count);
					while (extractIndexList.contains(randIndex)) {
						randIndex = random.nextInt((int) count);
					}
					extractIndexList.add(randIndex);
				}
			}
		}

		/**
		 * 通过SparkContext的broadcast方法传入要广播的变量即可
		 */
		final Broadcast<Map<String, Map<String, List<Integer>>>> dateHourExtractMapBroadcast = sc.broadcast(dateHourExtractMap);

		System.out.println(dateHourExtractMap);
		/**
		 * 第四步：遍历每天每小时的session，根据随机索引进行抽取
		 */
		//根据事件分组<dateHour, (session aggrInfo)>
		JavaPairRDD<String, Iterable<String>> time2sessionsRDD = time2SessionidRDD.groupByKey();
		//遍历每天每小时的session，若发现某个session在该小时随机抽取的索引
		// 中，则直接写入MySQL random_extract_session表，将抽取出来的
		// session_id返回RDD去Join访问行为明细数据并写入session_detail表中
		//返回结果为<session_id, session_id>y以便能够join
		JavaPairRDD<String, String> extractSessionIdsRDD = time2sessionsRDD.flatMapToPair((PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>) tumple2 -> {
			//返回结果
			List<Tuple2<String, String>> extractSessionIds = new ArrayList<>();
			String dateHour = tumple2._1;
			String date = dateHour.split("_")[0];
			String hour = dateHour.split("_")[1];
			Iterator<String> iterator = tumple2._2.iterator();

			/**
			 * 获取广播变量
			 */
			Map<String, Map<String, List<Integer>>> dateHourExtractMap1 = dateHourExtractMapBroadcast.value();
			//拿到某天date的某个小时hour随意抽取的索引
			List<Integer> extractIndexList = dateHourExtractMap1.get(date).get(hour);
			//用于将随机抽取结果写入数据库
			ISessionRandomExtractDAO sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO();

			int index = 0;
			while (iterator.hasNext()) {
				String sessionAggrInfo = iterator.next();
				if (extractIndexList.contains(index)) {
					//写数据库
					SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
					sessionRandomExtract.setTaskid(taskid);
					String sessionId = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
					sessionRandomExtract.setSessionid(sessionId);
					sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_START_TIME));
					sessionRandomExtract.setSearchKeyWords(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
					sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));
					sessionRandomExtractDAO.insert(sessionRandomExtract);
					//将sessionid加入列表
					extractSessionIds.add(new Tuple2<>(sessionId, sessionId));
				}
				index++;
			}
			return extractSessionIds;
		});
		/**
		 * 第六步：根据抽取的session_id与原始数据join拿到每个session的明
		 * 细信息并写入数据库。
		 * 注意：由于actionRDD是JavaRDD<Row>类型，因此采用方法getSessionid2ActionRdd
		 * 方法将其转换为JavaPairRDD<sessionid, Row>
		 */
		//JavaPairRDD<String, Row> sessionid2ActionRDD = getSessionid2ActionRdd(actionRDD);
		JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD = extractSessionIdsRDD.join(sessionid2ActionRDD);
		extractSessionDetailRDD.foreach((VoidFunction<Tuple2<String, Tuple2<String, Row>>>) tuple -> {
			Row row = tuple._2._2;
			SessionDetail detail = new SessionDetail();
			detail.setTaskid(taskid);
			detail.setUserid(row.getLong(1));
			detail.setSessionid(row.getString(2));
			detail.setPageid(row.getLong(3));
			detail.setActionTime(row.getString(4));
			detail.setSearchKeywords(row.getString(5));
			try {
				detail.setClickProductId(row.getLong(6));
			} catch (NullPointerException e) {
				detail.setClickProductId(-1);
			}
			try {
				detail.setClickCategoryId(row.getLong(7));
			} catch (NullPointerException e) {
				detail.setClickCategoryId(-1);
			}

			detail.setOrderCategoryIds(row.getString(8));
			detail.setOrderProductIds(row.getString(9));
			detail.setPayCategoryIds(row.getString(10));
			detail.setPayProductIds(row.getString(11));
			ISessionDetialDAO sessionDetialDAO = DAOFactory.getSessionDetailDAO();
			sessionDetialDAO.insert(detail);
		});
	}

	/**
	 * 将JavaRDD<Row>映射为JavaPairRDD<sessionid, Row>
	 */
	private static JavaPairRDD<String, Row> getSessionid2ActionRdd(JavaRDD<Row> actionRDD) {
		return actionRDD.mapToPair((PairFunction<Row, String, Row>) row -> {
			String sessionId = row.getString(2);
			return new Tuple2<>(sessionId, row);
		});
	}

	/**
	 * 获取Top10热门品类
	 */
	private static List<Tuple2<CategorySortKey, String>> getTop10Category(JavaPairRDD<String, Row> sessionid2DetailRDD, long taskId) {
		//第一步：获取符合条件的session访问过的所有品类的访问明细 重构
		//JavaPairRDD<String, Row> sessionid2DetailRDD = getSession2DetailRDD(filteredSessionid2AggrInfoRDD, sessionid2ActionRdd);

		//第二步：计算出session访问(点击、下单、支付)过的所有品类的Id
		JavaPairRDD<Long, Long> categoryIdRDD = sessionid2DetailRDD.flatMapToPair((PairFlatMapFunction<Tuple2<String, Row>, Long, Long>) tuple -> {
			Row detail = tuple._2;
			List<Tuple2<Long, Long>> list = new ArrayList<>();
			Long clickCategoryId = null;
			try {
				clickCategoryId = detail.getLong(6);
			} catch (Exception e) {
			}
			if (clickCategoryId != null) {
				list.add(new Tuple2<>(clickCategoryId, clickCategoryId));
			}

			String orderCategoryIds = detail.getString(8);
			if (orderCategoryIds != null) {
				String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
				for (int i = 0; i < orderCategoryIdsSplited.length; i++) {
					list.add(new Tuple2<>(Long.parseLong(orderCategoryIdsSplited[i]), Long.parseLong(orderCategoryIdsSplited[i])));
				}
			}
			String payCategoryIds = detail.getString(10);
			if (payCategoryIds != null) {
				String[] payCategoryIdsSplited = payCategoryIds.split(",");
				for (int i = 0; i < payCategoryIdsSplited.length; i++) {
					list.add(new Tuple2<>(Long.parseLong(payCategoryIdsSplited[i]), Long.parseLong(payCategoryIdsSplited[i])));
				}
			}
			return list;
		});

		/** 必须要进行去重：如果不去重会出现重复categoryId，导致最后结果出现重复 */
		categoryIdRDD = categoryIdRDD.distinct();

		//第三步：计算各品类的点击、下单和支付次数
		/**
		 * 计算各个品类的点击次数
		 * 这里完整数据进行过滤，多过滤出点击行为额数据，而点击行为只是总数据的
		 * 一小部分，所以过滤以后的RDD每个分区数据量很可能不均匀。
		 * 因此可以通过coalesce减少过滤后的分区数量，这里分区数量压缩为100个。
		 *
		 * 注意：由于这里是本地模式运行，不用去设置分区和并行度数量，而且对并
		 * 行度和分区数据量有一定内部优化，所以本地测试时可以不适用coalesce算
		 * 子。
		 */
		JavaPairRDD<String, Row> clickActionRDD = sessionid2DetailRDD.filter((Function<Tuple2<String, Row>, Boolean>) tuple -> {
			Row detail = tuple._2;
			Long clickCategoryId = null;
			try {
				clickCategoryId = detail.getLong(6);
			} catch (Exception e) {
			}
			return clickCategoryId != null;
		}).coalesce(100);
		JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD.mapToPair((PairFunction<Tuple2<String, Row>, Long, Long>) tuple -> {
			long clickCategoryId = tuple._2.getLong(6);
			return new Tuple2<>(clickCategoryId, 1L);
		});
		JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey((Function2<Long, Long, Long>) (v1, v2) -> v1 + v2);
		//数据倾斜解决方案四：随机Key实现双重聚合
		/** 第一步：给每个Key打一个随机数*/
		clickCategoryIdRDD.mapToPair((PairFunction<Tuple2<Long, Long>, String, Long>) tuple2 -> {
			Random rand = new Random();
			int prefix = rand.nextInt(10);
			return new Tuple2<String, Long>(prefix + "_" + tuple2._2, tuple2._2);
		})
				/** 第二步：局部聚合*/
				.reduceByKey((Function2<Long, Long, Long>) (v1, v2) -> {
					return v1 + v2;
				})
				/** 第三步：去除Key的前缀*/
				.mapToPair((PairFunction<Tuple2<String, Long>, Long, Long>) tuple2 -> {
					Long categoryId = Long.valueOf(tuple2._1.split("_")[1]);
					return new Tuple2<>(categoryId, tuple2._2);
				})
				/** 第四步：全局聚合*/
				.reduceByKey((Function2<Long, Long, Long>) (v1, v2) -> {
					return v1 + v2;
				});

		//计算各个品类下单的次数
		JavaPairRDD<String, Row> orderActionRDD = sessionid2DetailRDD.filter((Function<Tuple2<String, Row>, Boolean>) tuple -> {
			Row detail = tuple._2;
			return detail.getString(8) != null;
		});
		JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair((PairFlatMapFunction<Tuple2<String, Row>, Long, Long>) tuple -> {
			String orderCategoryIds = tuple._2.getString(8);
			String[] split = orderCategoryIds.split(",");
			List<Tuple2<Long, Long>> list = new ArrayList<>();
			for (int i = 0; i < split.length; i++) {
				list.add(new Tuple2<>(Long.parseLong(split[i]), 1L));
			}
			return list;
		});
		JavaPairRDD<Long, Long> orderCategoryId2CountRDD = orderCategoryIdRDD.reduceByKey((Function2<Long, Long, Long>) (v1, v2) -> v1 + v2);
		//计算各个品类支付的次数
		JavaPairRDD<String, Row> payActionRDD = sessionid2DetailRDD.filter((Function<Tuple2<String, Row>, Boolean>) tuple -> {
			Row detail = tuple._2;
			return detail.getString(10) != null;
		});
		JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair((PairFlatMapFunction<Tuple2<String, Row>, Long, Long>) tuple -> {
			String orderCategoryIds = tuple._2.getString(10);
			String[] split = orderCategoryIds.split(",");
			List<Tuple2<Long, Long>> list = new ArrayList<>();
			for (int i = 0; i < split.length; i++) {
				list.add(new Tuple2<>(Long.parseLong(split[i]), 1L));
			}
			return list;
		});
		JavaPairRDD<Long, Long> payCategoryId2CountRDD = payCategoryIdRDD.reduceByKey((Function2<Long, Long, Long>) (v1, v2) -> v1 + v2);

		//第四步：join各品类与它的点击、下单和支付的次数
		//categoryIdRDD中是包含所有符合条件的session访问过的品品类id，但
		// 是上面计算出来的三份是各品类的点击、下单和支付次数，可能不是包含所
		// 有品类的，比如有的品类就只是被点击过但是没有人下单和支付，所以这里
		// 就不能使用join操作而是使用leftJoin，这样如果categoryIdRDD不能
		// join到自己的某条数据时，那么categoryIdRDD还是要保留下来的，只不
		// 过没有join到的那条数据就是0
		JavaPairRDD<Long, Tuple2<Long, com.google.common.base.Optional<Long>>> tmpJoinRDD = categoryIdRDD.leftOuterJoin(clickCategoryId2CountRDD);
		//由于leftOuterJoin时右边可能没有值，因此类型为Optional<Long>
		JavaPairRDD<Long, String> categoryId2CountRDD = tmpJoinRDD
				.mapToPair((PairFunction<Tuple2<Long, Tuple2<Long, com.google.common.base.Optional<Long>>>, Long, String>) tuple -> {
					Long categoryId = tuple._1;
					Long clickCount = 0L;
					if (tuple._2._2.isPresent()) {
						clickCount = tuple._2._2.get();
					}
					String value = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|"
							+ Constants.FIELD_CLICK_COUNT + "=" + clickCount;
					return new Tuple2<>(categoryId, value);
				})
				.leftOuterJoin(orderCategoryId2CountRDD)
				.mapToPair((PairFunction<Tuple2<Long, Tuple2<String, com.google.common.base.Optional<Long>>>, Long, String>) tuple -> {
					Long categoryId = tuple._1;
					String value = tuple._2._1;
					Long orderCount = 0L;
					if (tuple._2._2.isPresent()) {
						orderCount = tuple._2._2.get();
					}
					value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;
					return new Tuple2<>(categoryId, value);
				})
				.leftOuterJoin(payCategoryId2CountRDD)
				.mapToPair((PairFunction<Tuple2<Long, Tuple2<String, com.google.common.base.Optional<Long>>>, Long, String>) tuple -> {
					Long categoryId = tuple._1;
					String value = tuple._2._1;
					Long payCount = 0L;
					if (tuple._2._2.isPresent()) {
						payCount = tuple._2._2.get();
					}
					value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;
					return new Tuple2<>(categoryId, value);
				});
		//第五步：根据自定义二次排序的Key进行映射、排序，反映射
		JavaPairRDD<CategorySortKey, String> sortedCategoryId2CountRDD = categoryId2CountRDD.mapToPair(((PairFunction<Tuple2<Long, String>, CategorySortKey, String>) tuple2 -> {
			Long categoryId = tuple2._1;
			String countInfo = tuple2._2;
			long clickCount = Long.parseLong(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
			long orderCount = Long.parseLong(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
			long payCount = Long.parseLong(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT));
			CategorySortKey categorySortKey = new CategorySortKey();
			categorySortKey.setClickCount(clickCount);
			categorySortKey.setOrderCount(orderCount);
			categorySortKey.setPayCount(payCount);
			return new Tuple2<>(categorySortKey, countInfo);
		}))
				.sortByKey(false);

		//第六步：用take(10)取出Top10并写入MySQL数据库中
		List<Tuple2<CategorySortKey, String>> top10CategoryList = sortedCategoryId2CountRDD.take(10);
		top10CategoryList.forEach(tuple2 -> {
			CategorySortKey categorySortKey = tuple2._1;
			String countInfo = tuple2._2;
			long categoryId = Long.parseLong(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID));

			Top10Category top10Category = new Top10Category();
			top10Category.setTaskId(taskId);
			top10Category.setCategoryId(categoryId);
			top10Category.setClickCount(categorySortKey.getClickCount());
			top10Category.setOrderCount(categorySortKey.getOrderCount());
			top10Category.setPayCount(categorySortKey.getPayCount());
			//插入数据库中
			ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();
			top10CategoryDAO.insert(top10Category);
		});
		return top10CategoryList;
	}

	/**
	 * 获取通过筛选条件的session访问明细数据 <sessionId, Row>
	 */
	private static JavaPairRDD<String, Row> getSession2DetailRDD(JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD, JavaPairRDD<String, Row> sessionid2ActionRdd) {
		return filteredSessionid2AggrInfoRDD.join(sessionid2ActionRdd)
				.mapToPair((PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>) tuple -> {
					return new Tuple2<>(tuple._1, tuple._2._2);
				});
	}

	/**
	 * 获取Top10活跃session
	 */
	private static void getTop10Session(JavaSparkContext sc, Long taskid, JavaPairRDD<String, Row> sessionid2DetailRDD, List<Tuple2<CategorySortKey, String>> top10CategoryList) {
		//第一步：将top10热门品类id，生成一份RDD<categoryId,categoryId>
		List<Tuple2<Long, Long>> categoryIds = new ArrayList<>();
		top10CategoryList.forEach(tuple -> {
			long categoryId = Long.parseLong(StringUtils.getFieldFromConcatString(tuple._2, "\\|", Constants.FIELD_CATEGORY_ID));
			categoryIds.add(new Tuple2<>(categoryId, categoryId));
		});
		JavaPairRDD<Long, Long> categoryIdRDD = sc.parallelizePairs(categoryIds);

		//第二步：计算Top10品类被各个session点击的次数 <category, "sessionid,count">
		JavaPairRDD<Long, String> categoryid2sessionCountRDD = sessionid2DetailRDD.groupByKey()
				.flatMapToPair((PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>) tuple2 -> {
					String sessionId = tuple2._1;
					Iterator<Row> iterator = tuple2._2.iterator();
					Map<Long, Long> categoryCountMap = new HashMap<>();
					//计算出当前session对每个品类的点击次数
					while (iterator.hasNext()) {
						Row row = iterator.next();
						Long categoryId = null;
						try {
							categoryId = row.getLong(6);
						} catch (Exception e) {
						}
						if (categoryId != null) {
							Long count = categoryCountMap.get(categoryId);
							if (count == null) {
								count = 1L;
							} else {
								count++;
							}
							categoryCountMap.put(categoryId, count);
						}
					}
					//返回结构<categoryId, sessionId,count>
					List<Tuple2<Long, String>> list = new ArrayList<>();
					categoryCountMap.entrySet().forEach(entry -> {
						list.add(new Tuple2<>(entry.getKey(), sessionId + "," + entry.getValue()));
					});
					return list;
				});

		//第三步：分组取TopN算法，获取每个品类的Top10活跃用户
		categoryIdRDD.join(categoryid2sessionCountRDD)
				.mapToPair((PairFunction<Tuple2<Long, Tuple2<Long, String>>, Long, String>) tuple2 -> {
					return new Tuple2<>(tuple2._1, tuple2._2._2);
				})
				.groupByKey()
				.flatMapToPair((PairFlatMapFunction<Tuple2<Long, Iterable<String>>, String, String>) tuple2 -> {
					long categoryId = tuple2._1;
					Iterator<String> sessions = tuple2._2.iterator();
					//分组取Top10活跃用户
					String[] top10Session = new String[10];
					while (sessions.hasNext()) {
						String sessionCount = sessions.next();
						String[] splits = sessionCount.split(",");
						String sessionId = splits[0];
						Long count = Long.valueOf(splits[1]);

						for (int i = 0; i < top10Session.length; i++) {
							if (top10Session[i] == null) {
								top10Session[i] = sessionCount;
								break;
							} else {
								Long _count = Long.valueOf(top10Session[i].split(",")[1]);
								if (count > _count) {
									System.arraycopy(top10Session, i, top10Session, i + 1, 9 - i);
									top10Session[i] = sessionCount;
								}
							}
						}
					}
					//将数据写入MySQL top10_category_session表
					List<Tuple2<String, String>> list = new ArrayList<>();
					for (int i = 0; i < top10Session.length; i++) {
						String sessionId = top10Session[i].split(",")[0];
						Long clickCount = Long.valueOf(top10Session[i].split(",")[1]);

						Top10Session session = new Top10Session();
						session.setTaskId(taskid);
						session.setCategoryId(categoryId);
						session.setSessionId(sessionId);
						session.setClickCount(clickCount);
						ITop10SessionDAO top10SessionDAO = DAOFactory.getTop10SessionDAO();
						top10SessionDAO.insert(session);
						list.add(new Tuple2<>(sessionId, sessionId));
					}
					return list;
				})
				//第四步：获取top10活跃session的明细数据，并写入数据库
				.join(sessionid2DetailRDD)
				.foreach((VoidFunction<Tuple2<String, Tuple2<String, Row>>>) tuple -> {
					Row row = tuple._2._2;
					SessionDetail detail = new SessionDetail();
					detail.setTaskid(taskid);
					detail.setUserid(row.getLong(1));
					detail.setSessionid(row.getString(2));
					detail.setPageid(row.getLong(3));
					detail.setActionTime(row.getString(4));
					detail.setSearchKeywords(row.getString(5));
					try {
						detail.setClickProductId(row.getLong(6));
					} catch (NullPointerException e) {
						detail.setClickProductId(-1);
					}
					try {
						detail.setClickCategoryId(row.getLong(7));
					} catch (NullPointerException e) {
						detail.setClickCategoryId(-1);
					}

					detail.setOrderCategoryIds(row.getString(8));
					detail.setOrderProductIds(row.getString(9));
					detail.setPayCategoryIds(row.getString(10));
					detail.setPayProductIds(row.getString(11));
					ISessionDetialDAO sessionDetialDAO = DAOFactory.getSessionDetailDAO();
					sessionDetialDAO.insert(detail);
				});
	}
}



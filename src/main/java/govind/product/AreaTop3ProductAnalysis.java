package govind.product;

import com.alibaba.fastjson.JSONObject;
import govind.conf.ConfigurationManager;
import govind.constant.Constants;
import govind.dao.IAreaTop3ProductDAO;
import govind.dao.ITaskDAO;
import govind.dao.impl.DAOFactory;
import govind.domain.AreaTop3Product;
import govind.domain.Task;
import govind.jdbc.JDBCHelper;
import govind.util.ParamUtils;
import govind.util.SparkUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

/**
 * 各区域Top3热门商品统计Spark作业
 */
@Slf4j
public class AreaTop3ProductAnalysis {
	public static void main(String[] args) {
		//1. 创建上下文
		SparkConf conf = new SparkConf()
				.setAppName(Constants.SPARK_APP_NAME_PRODUCT);
		SparkUtils.setMaster(conf);

		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = SparkUtils.getSQLContext(sc);

		//sqlContext.setConf("spark.sql.shuffle.partitions", "1000");
		//设置为2G
		//sqlContext.setConf("spark.sql.autoBroadcaseJoinThreshold", "20971520");
		//2. 准备模拟数据
		SparkUtils.mockData(sc, sqlContext);

		//3. 查询任务，获取任务参数
		Long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PRODUCT);
		if (taskid == null) {
			log.info("{}: 找不到任务id[{}]", new Date(), taskid);
			return;
		}

		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		Task task = taskDAO.findById(taskid);
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

		//4. 查询用户指定日期范围内的数据 => 技术点：基础数据源使用
		JavaPairRDD<Long, Row> cityid2ClickActionRDD = getCityid2ClickActionRDDByDate(sqlContext, startDate, endDate);

		//5. 从MySQL中查询城市信息 =》技术点：异步数据源使用
		JavaPairRDD<Long, Row> cityid2CityInfoRDD = getCityid2CityInfoRDD(sqlContext);

		//6. 生成点击商品基础信息临时表 技术点：将RDD转换为DataFrame并注册临时表
		joinAndGenerateClickProductBasicTable(sqlContext, cityid2ClickActionRDD, cityid2CityInfoRDD);

		/**
		 * 针对group by area,product_id后的数据：
		 * 1 北京 <city_id, city_name>
		 * 2 上海
		 * 希望得到的结果是：1:北京,2:上海,...。为此需要定义两个函数：
		 * 1、UDF函数：concat2()，根据指定分隔符拼接一条记录的两个字段；
		 * 2、UDAF函数:group_concat_distinct()，将一个分组中的多个字段值用
		 * 	 逗号拼接起来，同时进行去重，不能出现：1:北京,2:上海,1:北京。
		 *
		 * 注册自定义函数 =》技术点：自定义UDF、UDAF
		 */
		sqlContext.udf().register("concat_long_string",
				new ConcatLongStringUDF(), DataTypes.StringType);
		sqlContext.udf().register("group_concat_distinct",
				new GroupConcatDistinctUDAF());

		sqlContext.udf().register("get_json_object", new GetJsonObjectUDF(), DataTypes.IntegerType);
		sqlContext.udf().register("random_prefix", new RandomPrefixUDF(), DataTypes.StringType);
		sqlContext.udf().register("remove_random_prefix", new RemoveRandomPrefixUDF(), DataTypes.StringType);

		//7. 生成各区域各商品点击次数的临时表 => 使用
		generateTempAreaProductClickCountTable(sqlContext);

		//8. 生成包含完整商品信息的各区域各商品点击次数的临时表
		generateTempAreaFullProductClickCountTable(sqlContext);

		//9. 使用开窗函数获取各个区域内点击次数排名前3的热门商品
		JavaRDD<Row> areaTop3ProductRDD = getAreaTop3ProductTable(sqlContext);

		//10. 将统计结果写入数据库
		//persistAreaTop3Product(taskid, areaTop3ProductRDD);

		sc.close();
	}

	/**
	 * 获取指定日期范围内的点击行为数据
	 */
	public static JavaPairRDD<Long, Row> getCityid2ClickActionRDDByDate(SQLContext sqlContext, String startDate, String endDate) {
		//从user_visit_action中查询用户访问行为数据，有两个限定条件：
		//1、click_product_id不为空；2、在用户指定日期范围内。
		String sql =
				"SELECT " +
					"city_id, " +
					"click_product_id product_id " +
					"FROM user_visit_action " +
				"WHERE click_product_id IS NOT NULL " +
					"AND action_time >= '" + startDate + "' " +
					"AND action_time <= '" + endDate + "' "
					;
		DataFrame clickActionDF = sqlContext.sql(sql);
		clickActionDF.show();
		return clickActionDF.javaRDD().mapToPair((PairFunction<Row, Long, Row>) row -> {
			long cityid = row.getLong(0);
			return new Tuple2<>(cityid, row);
		});
	}

	/**
	 * 使用Spark SQL从MySQL中查询城市信息，并返回RDD
	 */
	public static JavaPairRDD<Long, Row> getCityid2CityInfoRDD(SQLContext sqlContext) {
		//构建MySQL连接配置信息(从配置文件中获取)
		String url;
		boolean isLocal = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if (isLocal) {
			url = ConfigurationManager.getProperty(Constants.JDBC_URL);
		} else {
			url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
		}
		Map<String, String> options = new HashMap<>();
		options.put("url", url);
		options.put("dbtable", "city_info");

		//通过SQLContext从MySQL查询数据
		DataFrame cityInfoDF = sqlContext.read().format("jdbc").options(options).load();
		cityInfoDF.show(10);
		return cityInfoDF.javaRDD().mapToPair((PairFunction<Row, Long, Row>) row -> {
			long cityid = (long) row.getInt(0);
			return new Tuple2<>(cityid, row);
		});
	}

	/**
	 * 生成点击商品基础信息临时表
	 */
	private static void joinAndGenerateClickProductBasicTable(SQLContext sqlContext, JavaPairRDD<Long, Row> cityid2ClickActionRDD, JavaPairRDD<Long, Row> cityid2CityInfoRDD) {
		JavaPairRDD<Long, Tuple2<Row, Row>> joinedRDD = (JavaPairRDD<Long, Tuple2<Row, Row>>) cityid2ClickActionRDD.join(cityid2CityInfoRDD);
		/**
		 * 需要将JavaPairRDD转换成一个JavaRDD<Row>才能将RDD转换DataFrame
		 */
		JavaRDD<Row> mappedRDD = joinedRDD.map((Function<Tuple2<Long, Tuple2<Row, Row>>, Row>) tuple -> {
			Long cityid = tuple._1;
			Row clickAction = tuple._2._1;
			Row cityInfo = tuple._2._2;

			long productid = clickAction.getLong(1);
			String cityName = cityInfo.getString(1);
			String area = cityInfo.getString(2);
			return RowFactory.create(cityid, cityName, area, productid);
		});
		//基于JavaRDD<Row>将其转换为DataFrame
		StructType dataTypes = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("city_id", DataTypes.LongType, true),
				DataTypes.createStructField("city_name", DataTypes.StringType, true),
				DataTypes.createStructField("area", DataTypes.StringType, true),
				DataTypes.createStructField("product_id", DataTypes.LongType, true)
		));
		DataFrame df = sqlContext.createDataFrame(mappedRDD, dataTypes);
		df.show(10);
		//注册为临时表tmp_clk_prod_basic
		df.registerTempTable("tmp_click_product_basic");
	}

	/**
	 * 生成各区域各商品点击次数的临时表
	 */
	private static void generateTempAreaProductClickCountTable(SQLContext sqlContext) {
		//按照area、productid两个字段分组，计算各区域各商品的点击次数，可以
		// 获得每个area下每个productid的城市信息拼接起来的串。
		String sql =
			"SELECT " +
				"area, " +
				"product_id, " +
				"count(*) click_count, " +
				"group_concat_distinct(concat_long_string(city_id, ':', city_name)) city_names " +
			"FROM tmp_click_product_basic " +
			"GROUP BY area, product_id";


		// 将查询结果注册为临时表
		DataFrame df = sqlContext.sql(sql);
		df.show(100);
		df.registerTempTable("tmp_area_product_click_count");
	}

	/**
	 * 生成包含完整商品信息的各区域各商品点击次数的临时表
	 */
	private static void generateTempAreaFullProductClickCountTable(SQLContext sqlContext) {
		//get_json_object函数从json串中获取指定的字段值
		//if函数判断若product_status为0则是自营商品否则是第三方商品
		String sql =
			"SELECT " +
				"tapcc.area, " +
				"tapcc.product_id, " +
				"tapcc.click_count, " +
				"tapcc.city_names, " +
				"pi.product_name, " +
				"if(get_json_object(pi.extend_info, 'product_status')=0, '自营商品', '第三方商品') product_status " +
			"FROM tmp_area_product_click_count tapcc " +
			"JOIN product_info pi ON tapcc.product_id = pi.product_id";

		DataFrame df = sqlContext.sql(sql);
		df.show();
		df.registerTempTable("tmp_area_fullprod_click_count");
	}

	/**
	 * 获取各区域Top3热门商品
	 * 开窗函数的子查询中按照area进行分组，给每个分组内的数据，按照点击次数降
	 * 序排列，打上一个组内的行号；紧接着外层查询中过滤出各个组内行号排名前3的
	 * 的数据，就得到了每个区域Top3的
	 * ROW_NUMBER窗口函数需要在Hive环境下运行
	 */
	private static JavaRDD<Row> getAreaTop3ProductTable(SQLContext sqlContext) {

		/**
		 * 区域：华北、华东、华南、华中、西北、西南、东北
		 * A级：华北和华东
		 * B级：华南和华中
		 * C级：西北和西南
		 * D级：东北
		 * 根据多个条件，不同条件对应不同的值
		 * case when then...when then...else ... end
		 */
		String sql =
				"SELECT " +
					"area, " +
					"CASE " +
						"WHEN area='华北' OR area='华东' THEN 'A级' " +
						"WHEN area='华中' OR area='华南' THEN 'B级' " +
						"WHEN area='西北' OR area='西南' THEN 'C级' " +
						"ELSE 'D级' " +
					"END area_level, " +
					"product_id, " +
					"click_count, " +
					"city_names, " +
					"product_name, " +
					"product_status " +
				"FROM(" +
					"SELECT " +
						"area, " +
						"product_id, " +
						"click_count, " +
						"city_names, " +
						"product_name, " +
						"product_status, " +
						"ROW_NUMBER() OVER(PARTITION BY area ORDER BY click_count DESC) rank " +
					"FROM tmp_area_fullprod_click_count" +
				") T " +
				"WHERE rank <=3";
		return sqlContext.sql(sql).javaRDD();
	}

	/**
	 * 将统计各个区域Top3写入MySQL数据库
	 */
	private static void persistAreaTop3Product(Long taskid, JavaRDD<Row> areaTop3ProductRDD) {
		areaTop3ProductRDD.foreachPartition((VoidFunction<Iterator<Row>>) partition -> {
			IAreaTop3ProductDAO areaTop3ProductDAO = DAOFactory.getAreaTop3ProductDAO();
			while (partition.hasNext()) {
				Row row = partition.next();
				String area = row.getString(0);
				String areaLevel = row.getString(1);
				long productid = Long.valueOf(row.getString(2));
				long clickCount = Long.valueOf(row.getString(3));
				String cityNames = row.getString(4);
				String productName = row.getString(5);
				String productStatus = row.getString(6);

				AreaTop3Product areaTop3Product = new AreaTop3Product();
				areaTop3Product.setTaskid(taskid);
				areaTop3Product.setArea(area);
				areaTop3Product.setAreaLevel(areaLevel);
				areaTop3Product.setProductid(productid);
				areaTop3Product.setClickCount(clickCount);
				areaTop3Product.setCityNames(cityNames);
				areaTop3Product.setProductName(productName);
				areaTop3Product.setProductStatus(productStatus);
				areaTop3ProductDAO.insert(areaTop3Product);
			}
		});
	}
}

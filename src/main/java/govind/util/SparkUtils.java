package govind.util;

import com.alibaba.fastjson.JSONObject;
import govind.conf.ConfigurationManager;
import govind.constant.Constants;
import govind.mock.MockData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Spark工具类
 */
public class SparkUtils {
	/**
	 * 根据当前是否是本地测试的配置，设置master
	 * @param conf
	 */
	public static void setMaster(SparkConf conf) {
		boolean isLocal = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if (isLocal) {
			conf.setMaster("local");
		}
	}

	/**
	 * 只有是本地模式时才会使用模拟生成的临时表
	 */
	public static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
		boolean isLocal = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if (isLocal) {
			MockData.simulateHiveTable(sc, sqlContext);
		}
	}

	/**
	 * 获取SQLContext，如果是本地测试环境就生成SQLContext对象，若是在生产
	 * 环境运行的话就生成HiveContext对象。
	 */
	public static SQLContext getSQLContext(JavaSparkContext sc) {
		boolean isLocal = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if (isLocal) {
			return new SQLContext(sc);
		} else {
			//生产上从Hive中查询数据
			return new HiveContext(sc);
		}
	}

	/**
	 * 获取指定日期范围内的用户访问行为数据
	 * @param sqlContext
	 * @param taskParam  任务参数
	 * @return 行为数据RDD
	 */
	public static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam) {
		String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
		String sql = "select * from user_visit_action " +
				"where date >='" + startDate + "' and date <= '" + endDate + "'";
		DataFrame actionDF = sqlContext.sql(sql);
		/**
		 * SparkSQL默认并行度是根据HDFS文件的block数量确定分区数量，为防止
		 * 分区数量少导致少量Task处理大量数据的情况，采用repartition进行重
		 * 分区。
		 */
		//return actionDF.javaRDD().repartition(1000);
		return actionDF.javaRDD();
	}
}

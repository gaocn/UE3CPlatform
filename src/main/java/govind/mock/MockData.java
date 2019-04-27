package govind.mock;

import govind.util.DateUtils;
import govind.util.NumberUtils;
import govind.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

/**
 * 模拟数据程序
 */
@Slf4j
public class MockData {
	/**
	 * 模拟数据
	 *
	 * @param sc
	 * @param hiveContext
	 */
	public static void simulateHiveTable(JavaSparkContext sc, SQLContext hiveContext) {
		ArrayList<Row> rows = new ArrayList<>();
		String[] searchKeywords = {"火锅", "蛋糕", "重庆辣子鸡", "重庆小面", "呷哺呷哺", "新辣道鱼火锅",
				"国贸大厦", "太古商场", "日本料理", "温泉"};
		String date = DateUtils.getTodayDate();
		String[] actions = {"search", "click", "order", "pay"};
		Random rand = new Random();

		for (int i = 0; i < 100; i++) {
			long userid = rand.nextInt(100);
			for (int j = 0; j < 10; j++) {
				String sessionId = UUID.randomUUID().toString().replace("-", "");
				String baseActionTime = date + " " + StringUtils.fillWithZero(String.valueOf(rand.nextInt(23)));
				for (int k = 0; k < rand.nextInt(100); k++) {
					long pageId = rand.nextInt(10);
					String actionTime = baseActionTime + ":" + StringUtils.fillWithZero(String.valueOf(rand.nextInt(59))) + ":" + StringUtils.fillWithZero(String.valueOf(rand.nextInt(59)));
					String searchKeyword = null;
					Long clickCategoryId = null;
					Long clickProductId = null;
					String orderCategoryIds = null;
					String orderProductIds = null;
					String payCategoryIds = null;
					String payProductIds = null;

					String action = actions[rand.nextInt(actions.length)];
					if ("search".equals(action)) {
						searchKeyword = searchKeywords[rand.nextInt(searchKeywords.length)];
					} else if ("click".equals(action)) {
						clickCategoryId = Long.valueOf(String.valueOf(rand.nextInt(100)));
						clickProductId = Long.valueOf(String.valueOf(rand.nextInt(100)));
					} else if ("order".equals(action)) {
						orderCategoryIds = String.valueOf(rand.nextInt(100));
						orderProductIds = String.valueOf(rand.nextInt(100));
					} else if ("pay".equals(action)) {
						payCategoryIds = String.valueOf(rand.nextInt(100));
						payProductIds = String.valueOf(rand.nextInt(100));
					}

					Row row = RowFactory.create(date, userid, sessionId, pageId, actionTime, searchKeyword,
							clickCategoryId, clickProductId, orderCategoryIds, orderProductIds, payCategoryIds,
							payProductIds, (long) rand.nextInt(10));
					rows.add(row);
				}
			}
		}
		JavaRDD<Row> rowsRDD = sc.parallelize(rows);

		StructType schema = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("action_time", DataTypes.StringType, true),
				DataTypes.createStructField("user_id", DataTypes.LongType, true),
				DataTypes.createStructField("session_id", DataTypes.StringType, true),
				DataTypes.createStructField("page_id", DataTypes.LongType, true),
				DataTypes.createStructField("action_name", DataTypes.StringType, true),
				DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
				DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
				DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
				DataTypes.createStructField("order_category_id", DataTypes.StringType, true),
				DataTypes.createStructField("order_product_id", DataTypes.StringType, true),
				DataTypes.createStructField("pay_category_id", DataTypes.StringType, true),
				DataTypes.createStructField("pay_product_id", DataTypes.StringType, true),
				DataTypes.createStructField("city_id", DataTypes.LongType, true)
		));
		DataFrame df = hiveContext.createDataFrame(rowsRDD, schema);
		df.registerTempTable("user_visit_action");
		df.show();

		//用户基础信息表
		rows.clear();
		String[] sexes = new String[]{"male", "female"};
		for (int i = 0; i < 100; i++) {
			long userid = i;
			String username = "user" + i;
			String name = "name" + i;
			int age = rand.nextInt(60);
			String professional = "professional" + rand.nextInt(100);
			String city = "city" + rand.nextInt(100);
			String sex = sexes[rand.nextInt(sexes.length)];

			Row row = RowFactory.create(userid, username, name, age, professional, city, sex);
			rows.add(row);
		}

		JavaRDD<Row> userRDD = sc.parallelize(rows);

		StructType userType = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("user_id", DataTypes.LongType, true),
				DataTypes.createStructField("username", DataTypes.StringType, true),
				DataTypes.createStructField("name", DataTypes.StringType, true),
				DataTypes.createStructField("age", DataTypes.IntegerType, true),
				DataTypes.createStructField("professional", DataTypes.StringType, true),
				DataTypes.createStructField("city", DataTypes.StringType, true),
				DataTypes.createStructField("sex", DataTypes.StringType, true)
		));

		DataFrame userDF = hiveContext.createDataFrame(userRDD, userType);
		for (Row row : userDF.take(1)) {
			log.info("{}", row);
		}
		userDF.registerTempTable("user_info");

		//生成product_info表
		rows.clear();
		int[] productStatus = new int[]{0, 1};
		for (int i = 0; i < 100; i++) {
			long productId = i;
			String productName = "product" + i;
			String extendInfo = "{\"product_status\":" + productStatus[rand.nextInt(2)] + "}";
			Row row = RowFactory.create(productId, productName, extendInfo);
			rows.add(row);
		}
		rowsRDD = sc.parallelize(rows);
		StructType productType = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("product_id", DataTypes.LongType, true),
				DataTypes.createStructField("product_name", DataTypes.StringType, true),
				DataTypes.createStructField("extend_info", DataTypes.StringType, true)
		));

		DataFrame productDF = hiveContext.createDataFrame(rowsRDD, productType);
		for(Row _row : productDF.take(3)) {
			System.out.println(_row);
		}
		productDF.registerTempTable("product_info");
	}
}

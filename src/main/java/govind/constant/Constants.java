package govind.constant;

/**
 * 常量封装，避免硬编码提高代码的可维护性、灵活性。
 */
public class Constants {

	/**
	 * 数据库配置相关常量
	 */
	public static String JDBC_DRIVER = "jdbc.driver";

	public static String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";

	public static String JDBC_URL = "jdbc.url";
	public static String JDBC_USER = "jdbc.user";
	public static String JDBC_PASSWORD = "jdbc.password";
	public static String JDBC_URL_PROD = "jdbc.url.prod";
	public static String JDBC_USER_PROD = "jdbc.user.prod";
	public static String JDBC_PASSWORD_PROD = "jdbc.password.prod";

	/**
	 * Spark作业相关常量
	 */
	public static String SPARK_APP_NAME_SESSION = "UserVisitSessionAnalysis";
	public static String SPARK_APP_NAME_PAGE = "PageOneStepConvertRateAnalysis";
	public static String SPARK_APP_NAME_PRODUCT = "AreaTop3ProductAnalysis";

	//运行模式：本地还是生产环境
	public static String SPARK_LOCAL = "spark.local";
	public static String SPARK_LOCAL_TASKID_SESSION = "spark.local.taskid.session";
	public static String SPARK_LOCAL_TASKID_PAGE = "spark.local.taskid.page";
	public static String SPARK_LOCAL_TASKID_PRODUCT = "spark.local.taskid.product";


	public static String FIELD_SESSION_ID = "sessionid";
	public static String FIELD_SEARCH_KEYWORDS = "searchKeywords";
	public static String FIELD_CLICK_CATEGORY_IDS = "clickCategoryIds";
	public static String FIELD_AGE = "age";
	public static String FIELD_PROFESSIONAL = "professional";
	public static String FIELD_CITY = "city";
	public static String FIELD_SEX = "sex";
	public static String FIELD_VISIT_LENGTH = "visitLength";
	public static String FIELD_STEP_LENGTH = "stepLength";
	public static final String FIELD_START_TIME = "startTime";
	public static final String FIELD_CLICK_COUNT = "clickCount";
	public static final String FIELD_ORDER_COUNT = "orderCount";
	public static final String FIELD_PAY_COUNT = "payCount";
	public static final String FIELD_CATEGORY_ID = "categoryId";

	/**
	 * 任务相关常量，代表JSON参数中KEY
	 */
	public static String PARAM_START_DATE = "startDate";
	public static String PARAM_END_DATE = "endDate";
	public static String PARAM_START_AGE = "startAge";
	public static String PARAM_END_AGE = "endAge";
	public static String PARAM_PROFESSIONALS = "professionals";
	public static String PARAM_CITIES = "cities";
	public static String PARAM_SEX = "sex";
	public static String PARAM_SEARCH_KEYOWORDS = "keywords";
	public static String PARAM_CATEGORY_IDS = "categoryIds";

	public static final String PARAM_TARGET_PAGE_FLOW = "pageFlow";

	/**
	 * session聚合统计的访问时长和访问步长
	 */
	public static String SESSION_COUNT = "sessionCount";
	public static String TIME_PERIOD_1s_3s = "1s_3s";
	public static String TIME_PERIOD_4s_6s = "4s_6s";
	public static String TIME_PERIOD_7s_9s = "7s_9s";
	public static String TIME_PERIOD_10s_30s = "10s_30s";
	public static String TIME_PERIOD_30s_60s = "30s_60s";
	public static String TIME_PERIOD_1m_3m = "1m_3m";
	public static String TIME_PERIOD_3m_10m = "3m_10m";
	public static String TIME_PERIOD_10m_30m = "10m_30m";
	public static String TIME_PERIOD_30m = "30m";
	public static String STEP_PERIOD_1_3 = "1_3 ";
	public static String STEP_PERIOD_4_6 = "4_6 ";
	public static String STEP_PERIOD_7_9 = "7_9";
	public static String STEP_PERIOD_10_30 = "10_30";
	public static String STEP_PERIOD_30_60 = "30_60";
	public static String STEP_PERIOD_60 = "60";


}

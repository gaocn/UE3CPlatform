package govind.jdbc;


import govind.conf.ConfigurationManager;
import govind.constant.Constants;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

@Slf4j
public class JDBCHelper {
	//静态代码块中，直接加载数据库驱动
	static {
		try {
			Class.forName(ConfigurationManager.getProperty(Constants.JDBC_DRIVER));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	//实现JDBCHelper的单例化，因为内部要封装数据库连接池，为了保证数据库连接
	// 池有且仅有一份，就需要通过单例模式保证只有一个JDBCHelper实例，在应用
	// 程序整个生命周期只会被创建一次。
	private static JDBCHelper instance;

	public static JDBCHelper getInstance() {
		if (instance == null) {
			synchronized (JDBCHelper.class) {
				if (instance == null) {
					instance = new JDBCHelper();
				}
			}
		}
		return instance;
	}

	/**
	 * 数据库连接池
	 */
	private LinkedList<Connection> datasource = new LinkedList<>();

	private JDBCHelper() {
		boolean isLocal = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		String url;
		String user;
		String password;
		if (isLocal) {
			url = ConfigurationManager.getProperty(Constants.JDBC_URL);
			user = ConfigurationManager.getProperty(Constants.JDBC_USER);
			password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
		} else {
			url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
			user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
			password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
		}

		int datasourceSize = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);
		//创建指定数量的数据库连接，并放入数据库连接池中
		for (int i = 0; i < datasourceSize; i++) {
			try {
				Connection connection = DriverManager.getConnection(url, user, password);
				datasource.push(connection);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		log.debug("创建{}个数据库连接到连接池中", datasourceSize);
	}

	/**
	 * 获取数据库连接池，当连接池中没有可用连接时则等待直到有空闲连接为止，为
	 * 保证线程安全添加同步关键字，当第一线程处于循环等待时，其他线程会被同步
	 * 关键字阻塞等待当前线程获取连接后，再依次进入代码尝试获取连接。
	 */
	public synchronized Connection getConnection() {
		while (datasource.size() == 0) {
			try {
				log.debug("无可用数据库连接，{}-{}进入等待状态", Thread.currentThread().getName(), Thread.currentThread().getId());
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return datasource.poll();
	}

	/**
	 * 返回数据库连接到连接池，以便重用
	 */
	public void release(Connection conn) {
		if (conn != null) {
			synchronized (this) {
				datasource.push(conn);
			}
			log.debug("", conn);
		}
	}

	/**
	 * 开发数据CRUD方法
	 * 1、执行增删改SQL语句；
	 * 2、执行查询SQL语句方法；
	 * 3、批量执行SQL语句方法；
	 */

	/**
	 * 执行增删改SQL语句
	 * @return 影响的行数
	 */
	public int executeUpdate(String sql, Object[] params) {
		Connection conn = null;
		PreparedStatement pstmt = null;
		int rtn = 0;
		try {
			conn = getConnection();
			pstmt = conn.prepareStatement(sql);
			for (int i = 0; params != null && i < params.length; i++) {
				pstmt.setObject(i+1, params[i]);
			}
			rtn = pstmt.executeUpdate();
		} catch (Exception e){
			log.info("执行SQL语句[{}]出现异常：{}", sql, e.getMessage());
		} finally {
			release(conn);
		}
		return rtn;
	}

	/**
	 * 执行查询回调语句
	 * @param sql
	 * @param params
	 * @param callBack 查询结果回调函数
	 */
	public void executeQuery(String sql, Object[] params, QueryCallBack callBack) {
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet resultSet = null;
		try {
			conn = getConnection();
			pstmt = conn.prepareStatement(sql);
			for (int i = 0; params != null && i < params.length; i++) {
				pstmt.setObject(i+1, params[i]);
			}
			resultSet = pstmt.executeQuery();
			callBack.process(resultSet);
		} catch (Exception e){
			log.info("执行SQL语句[{}]出现异常：{}", sql, e.getMessage());
		} finally {
			release(conn);
		}
	}

	/**
	 * 内部类：查询回调接口
	 */
	public static interface QueryCallBack{
		/**
		 * 处理查询结果
		 * @param rs 查询结果
		 */
		void process(ResultSet rs) throws Exception;
	}

	/**
	 * 批量执行SQL语句
	 * @param sql
	 * @param paramList
	 * @return 每条SQL语句影响的行数
	 */
	public int[] executeBatch(String sql, List<Object[]> paramList) {
		int[] rtn = null;
		Connection conn = null;
		PreparedStatement pstmt = null;

		try {
			conn = getConnection();
			//实现批量提交，需要取消自动提交
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement(sql);

			for (Object[] params : paramList) {
				for (int i = 0; params != null && i < params.length; i++) {
					pstmt.setObject(i+1, params[i]);
				}
				pstmt.addBatch();
			}
			//批量执行SQL语句
			rtn = pstmt.executeBatch();
			//提交批量SQL语句
			conn.commit();

		} catch (Exception e) {
			log.info("执行SQL语句[{}]出现异常：{}", sql, e.getMessage());
		} finally {
			release(conn);
		}
		return rtn;
	}
}

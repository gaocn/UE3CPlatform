package govind.dao.impl;

import govind.dao.*;

/**
 * Task DAO工厂类
 */
public class DAOFactory {
	/**
	 * 获取任务管理DAO
	 */
	public static ITaskDAO getTaskDAO(){
		return new TaskDAOImpl();
	}

	/**
	 * 获取session聚合统计DAO
	 */
	public static ISessionAggrStatDAO getSessionAggrStatDAO() {
		return new SessionAggrStatDAOImpl();
	}

	public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
		return new SessionRandomExtractDAOImpl();
	}

	public static ISessionDetialDAO getSessionDetailDAO() {
		return new SessionDetailDAOImpl();
	}

	public static ITop10CategoryDAO getTop10CategoryDAO() {
		return new Top10CategoryDAOImpl();
	}

	public static ITop10SessionDAO getTop10SessionDAO() {
		return new Top10SessionDAOImpl();
	}

	public static IPageConvertRateDAO getPageConvertRateDAO() {
		return new PageConvertRateDAOImpl();
	}

	public static IAreaTop3ProductDAO getAreaTop3ProductDAO() {
		return new AreaTop3ProductDAOImpl();
	}
	public static IAdUserClickCountDAO getAdUserClickCountDAO() {
		return new AdUserClickCountDAOImpl();
	}

	public static IAdBlackListDAO getAdBlackListDAO() {
		return new AdBlackListDAOImpl();
	}

	public static IAdStatDAO getAdStatDAO() {
		return new AdStatDAOImpl();
	}
	public static IAdProvinceTop3DAO getAdProvinceTop3DAO() {
		return new AdProvinceTop3DAOImpl();
	}

	public static IAdClickTrendDAO getAdClickTrendDAO() {
		return new AdClickTrendDAOImpl();
	}
 }

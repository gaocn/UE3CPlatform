package govind.dao.impl;

import govind.dao.ISessionDetialDAO;
import govind.domain.SessionDetail;
import govind.jdbc.JDBCHelper;

public class SessionDetailDAOImpl implements ISessionDetialDAO {
	@Override
	public void insert(SessionDetail sessionDetail) {
		String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";
		Object[] params = new Object[] {
			sessionDetail.getTaskid(),
			sessionDetail.getUserid(),
			sessionDetail.getSessionid(),
			sessionDetail.getPageid(),
			sessionDetail.getActionTime(),
			sessionDetail.getSearchKeywords(),
			sessionDetail.getClickCategoryId(),
			sessionDetail.getClickProductId(),
			sessionDetail.getOrderCategoryIds(),
			sessionDetail.getOrderProductIds(),
			sessionDetail.getPayCategoryIds(),
			sessionDetail.getPayProductIds(),
		};
		JDBCHelper.getInstance().executeUpdate(sql, params);
	}
}

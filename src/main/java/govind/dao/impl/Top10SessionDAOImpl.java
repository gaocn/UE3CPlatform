package govind.dao.impl;

import govind.dao.ITop10SessionDAO;
import govind.domain.Top10Session;
import govind.jdbc.JDBCHelper;

public class Top10SessionDAOImpl implements ITop10SessionDAO {
	@Override
	public void insert(Top10Session top10Session) {
		String sql = "insert into top10_session values(?,?,?,?)";
		Object[] params = new Object[]{
			top10Session.getTaskId(),
				top10Session.getCategoryId(),
				top10Session.getSessionId(),
				top10Session.getClickCount()
		};

		JDBCHelper.getInstance().executeUpdate(sql, params);
	}
}

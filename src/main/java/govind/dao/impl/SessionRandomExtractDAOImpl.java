package govind.dao.impl;

import govind.dao.ISessionRandomExtractDAO;
import govind.domain.SessionRandomExtract;
import govind.jdbc.JDBCHelper;

public class SessionRandomExtractDAOImpl implements ISessionRandomExtractDAO {
	@Override
	public void insert(SessionRandomExtract sessionRandomExtract) {
		String sql = "insert into session_random_extract values(?,?,?,?,?)";
		Object[] params = new Object[] {
			sessionRandomExtract.getTaskid(),
				sessionRandomExtract.getSessionid(),
				sessionRandomExtract.getStartTime(),
				sessionRandomExtract.getSearchKeyWords(),
				sessionRandomExtract.getClickCategoryIds()
		};
		JDBCHelper.getInstance().executeUpdate(sql, params);
	}
}

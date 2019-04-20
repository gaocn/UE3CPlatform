package govind.dao.impl;

import govind.dao.ISessionAggrStatDAO;
import govind.domain.SessionAggrStat;
import govind.jdbc.JDBCHelper;

public class SessionAggrStatDAOImpl implements ISessionAggrStatDAO {
	@Override
	public void insert(SessionAggrStat sessionAggrStat) {
		String sql = "insert into session_aggr_stat values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		Object[] params = new Object[]{
			sessionAggrStat.getTaskid(),sessionAggrStat.getSessionCount(),
				sessionAggrStat.getVisiLength1s3sRation(),
				sessionAggrStat.getVisiLength4s6sRation(),
				sessionAggrStat.getVisiLength7s9sRation(),
				sessionAggrStat.getVisiLength10s30sRation(),
				sessionAggrStat.getVisiLength30s60sRation(),
				sessionAggrStat.getVisiLength1m3mRation(),
				sessionAggrStat.getVisiLength3m10mRation(),
				sessionAggrStat.getVisiLength10m30mRation(),
				sessionAggrStat.getVisiLength30mRation(),
				sessionAggrStat.getStepLength13Ration(),
				sessionAggrStat.getStepLength46Ration(),
				sessionAggrStat.getStepLength79Ration(),
				sessionAggrStat.getStepLength1030Ration(),
				sessionAggrStat.getStepLength3060Ration(),
				sessionAggrStat.getStepLength60Ration()
		};
		JDBCHelper.getInstance().executeUpdate(sql, params);
	}
}

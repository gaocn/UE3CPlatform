package govind.dao.impl;

import govind.dao.IAdBlackListDAO;
import govind.domain.AdBlackList;
import govind.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class AdBlackListDAOImpl implements IAdBlackListDAO {
	@Override
	public void insert(AdBlackList adBlackList) {
		String sql = "insert into ad_blacklist values(?)";
		Object[] params = new Object[]{
				adBlackList.getUserid()
		};
		JDBCHelper.getInstance().executeUpdate(sql, params);
	}

	@Override
	public void insertBatch(List<AdBlackList> adBlacikLists) {
		String sql = "insert into ad_blacklist values(?)";
		List<Object[]> paramsList = new ArrayList<>();
		for (AdBlackList ad : adBlacikLists) {
			Object[] params = new Object[]{
				ad.getUserid()
			};
			paramsList.add(params);
		}
		JDBCHelper.getInstance().executeBatch(sql, paramsList);
	}

	@Override
	public List<AdBlackList> finall() {
		String sql = "select * from ad_blacklist";
		List<AdBlackList> adBlackLists = new ArrayList<>();
		JDBCHelper.getInstance().executeQuery(sql, null, new JDBCHelper.QueryCallBack() {
			@Override
			public void process(ResultSet rs) throws Exception {
				while (rs.next()) {
					AdBlackList adBlackList = new AdBlackList();
					adBlackList.setUserid(rs.getInt(1));
					adBlackLists.add(adBlackList);
				}
			}
		});
		return adBlackLists;
	}
}

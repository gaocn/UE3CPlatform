package govind.dao.impl;

import govind.dao.IAdStatDAO;
import govind.domain.AdStat;
import govind.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class AdStatDAOImpl implements IAdStatDAO {
	@Override
	public void insertAndUpdateBatch(List<AdStat> adStats) {
		List<AdStat> insertAdStats = new ArrayList<>();
		List<AdStat> updateAdStats = new ArrayList<>();

		String querySQl = "select count(*) from ad_stat where " +
				"date=?" +
				"AND province=?" +
				"AND city=?" +
				"AND ad_id=?";
		adStats.forEach(adStat -> {
			Object[] params = new Object[] {
					adStat.getDate(),
					adStat.getProvince(),
					adStat.getCity(),
					adStat.getAdid()
			};
			JDBCHelper.getInstance().executeQuery(querySQl, params, rs -> {
				if (rs.next()) {
					int count = rs.getInt(0);
					if (count > 0) {
						updateAdStats.add(adStat);
					} else {
						insertAdStats.add(adStat);
					}
				}
			});
		});

		String insertSql = "insert into ad_stat values(?,?,?,?,?)";
		List<Object[]> paramsList = new ArrayList<>();

		for(AdStat adStat : insertAdStats) {
			Object[] params = new Object[] {
				adStat.getDate(),
					adStat.getProvince(),
					adStat.getCity(),
					adStat.getProvince(),
					adStat.getClickCount()
			};
			paramsList.add(params);
		}
		JDBCHelper.getInstance().executeBatch(insertSql, paramsList);

		String updateSql = "update ad_stat set click_count=? where " +
				"date=?" +
				"AND province=?" +
				"AND city=?" +
				"AND ad_id=?";
		paramsList.clear();
		for(AdStat adStat : updateAdStats) {
			Object[] params = new Object[] {
					adStat.getDate(),
					adStat.getProvince(),
					adStat.getCity(),
					adStat.getProvince(),
					adStat.getClickCount()
			};
			paramsList.add(params);
		}
		JDBCHelper.getInstance().executeBatch(updateSql, paramsList);

	}
}

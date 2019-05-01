package govind.dao.impl;

import govind.dao.IAdClickTrendDAO;
import govind.domain.AdClickTrend;
import govind.jdbc.JDBCHelper;
import jdk.nashorn.internal.scripts.JD;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * 广告点击趋势DAO
 */
public class AdClickTrendDAOImpl implements IAdClickTrendDAO {
	@Override
	public void insertAndUpdateBatch(List<AdClickTrend> adClickTrends) {
		//区分哪些是要插入，哪些是要更新，通常来说，同一个key包含多条数据，而
		// 相同的值会在同一个分区，因此插入不会出现重复。
		//但是根据业务需求来，若存在重复插入问题，一个解决方案是在数据库表中添
		// 加create_time字段，在J2EE系统查询时，直接根据最新数据查询就行，
		// 从而规避掉重复插入问题
		List<AdClickTrend> updateACT = new ArrayList<>();
		List<AdClickTrend> insertACT = new ArrayList<>();

		String querySql = "SELECT COUNT(*) FROM ad_click_trend " +
				"WHERE date=? AND hour=? AND minute=? AND ad_id=?";
		for (AdClickTrend act : adClickTrends) {
			Object[] params = new Object[] {
				act.getDate(),act.getHour(),
					act.getMinute(), act.getAdid()
			};
			JDBCHelper.getInstance().executeQuery(querySql, params, rs -> {
				if (rs.next()) {
					int count = rs.getInt(1);
					if (count > 0) {
						updateACT.add(act);
					} else {
						insertACT.add(act);
					}
				}
			});
		}

		//执行更新操作
		String updateSql = "UPDATE ad_click_trend set click_count " +
				"WHERE date=? AND hour=? AND minute=? AND ad_id=?";
		List<Object[]> updateParams = new ArrayList<>();
		for (AdClickTrend act : updateACT) {
			Object[] param = new Object[] {
					act.getDate(), act.getHour(),
					act.getMinute(), act.getAdid()
			};
			updateParams.add(param);
		}
		JDBCHelper.getInstance().executeBatch(updateSql, updateParams);

		//执行更新操作
		String insertSql = "INSERT INTO ad_click_trend VALUES(?,?,?,?,?)";
		List<Object[]> insertParams = new ArrayList<>();
		for (AdClickTrend act : insertACT) {
			Object[] param = new Object[] {
					act.getDate(), act.getHour(),
					act.getMinute(), act.getAdid()
			};
			insertParams.add(param);
		}
		JDBCHelper.getInstance().executeBatch(insertSql, insertParams);
	}
}

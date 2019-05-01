package govind.dao.impl;

import govind.dao.IAdUserClickCountDAO;
import govind.domain.AdUserClickCount;
import govind.jdbc.JDBCHelper;
import govind.model.AdUserCountQueryResult;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class AdUserClickCountDAOImpl implements IAdUserClickCountDAO {
	@Override
	public void updateBatch(List<AdUserClickCount> adUserClickCounts) {
		List<AdUserClickCount> insertAdUserClickCounts = new ArrayList<>();
		List<AdUserClickCount> updateAdUserClickCounts = new ArrayList<>();

		//首先对用户广告点击量进行分类
		String selectSql = "SELECT click_count FROM ad_user_click_count " +
				"WHERE date=? AND user_id=? AND ad_id=?";
		Object[] selectParams = null;
		for (AdUserClickCount adUserClickCount : adUserClickCounts) {
			//这里引入model.AdUserCountQueryResult封装查询结果
			final AdUserCountQueryResult adUserCountQueryResult = new AdUserCountQueryResult();

			selectParams = new Object[]{
					adUserClickCount.getDate(),
					adUserClickCount.getUserid(),
					adUserClickCount.getAdid()
			};
			JDBCHelper.getInstance().executeQuery(selectSql, selectParams, new JDBCHelper.QueryCallBack() {
				@Override
				public void process(ResultSet rs) throws Exception {
					if (rs.next()) {
						int count = rs.getInt(1);
						adUserCountQueryResult.setCount(count);
					}
				}
			});

			if (adUserCountQueryResult.getCount() > 0) {
				long updateCount = adUserCountQueryResult.getCount() + adUserClickCount.getClickCount();
				adUserClickCount.setClickCount(updateCount);
				updateAdUserClickCounts.add(adUserClickCount);
			} else {
				insertAdUserClickCounts.add(adUserClickCount);
			}
		}

		//2. 执行批量插入
		String insertSQL = "insert into ad_user_click_count values(?,?,?,?)";
		List<Object[]> insertParamList = new ArrayList<>();
		for (AdUserClickCount adUserClickCount : insertAdUserClickCounts) {
			Object[] insertParams = new Object[]{
					adUserClickCount.getDate(),
					adUserClickCount.getUserid(),
					adUserClickCount.getAdid(),
					adUserClickCount.getClickCount()
			};
			insertParamList.add(insertParams);
		}
		JDBCHelper.getInstance().executeBatch(insertSQL, insertParamList);

		//3. 执行批量更新操作
		String updateSql = "update ad_user_click_count set click_count=? " +
				"where date=? and user_id=? and ad_id=?";
		List<Object[]> updateParamList = new ArrayList<>();
		for (AdUserClickCount adUserClickCount : updateAdUserClickCounts) {
			Object[] updateParams = new Object[]{
					adUserClickCount.getClickCount(),
					adUserClickCount.getDate(),
					adUserClickCount.getUserid(),
					adUserClickCount.getAdid()
			};
			updateParamList.add(updateParams);
		}
		JDBCHelper.getInstance().executeBatch(updateSql, updateParamList);
	}

	@Override
	public int findClickCountByKeys(String date, long userid, long adid) {
		String sql = "select click_count from ad_user_click_count " +
				"where  date=? and user_id=? and ad_id=?";
		Object[] params = new Object[] {
				date, userid, adid
		};
		final int[] clickCount = {0};
		JDBCHelper.getInstance().executeQuery(sql, params, new JDBCHelper.QueryCallBack() {
			@Override
			public void process(ResultSet rs) throws Exception {
				if (rs.next()) {
					 clickCount[0] = rs.getInt(0);
				}
			}
		});
		return clickCount[0];
	}
}

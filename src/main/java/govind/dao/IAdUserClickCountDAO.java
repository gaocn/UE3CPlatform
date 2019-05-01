package govind.dao;

import govind.domain.AdUserClickCount;

import java.util.List;

/**
 * 用户广告点击量DAO接口
 */
public interface IAdUserClickCountDAO {
	void updateBatch(List<AdUserClickCount> adUserClickCounts);

	int findClickCountByKeys(String date, long userid, long adid);
}

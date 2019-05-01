package govind.dao;

import govind.domain.AdStat;

import java.util.List;

/**
 * 广告实时统计DOA
 */
public interface IAdStatDAO {
	void insertAndUpdateBatch(List<AdStat> adStats);
}

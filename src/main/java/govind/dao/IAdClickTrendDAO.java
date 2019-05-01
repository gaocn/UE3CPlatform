package govind.dao;

import govind.domain.AdClickTrend;

import java.util.List;

public interface IAdClickTrendDAO {
	void insertAndUpdateBatch(List<AdClickTrend> adClickTrends);
}

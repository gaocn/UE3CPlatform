package govind.dao;

import govind.domain.AdProvinceTop3;

import java.util.List;

public interface IAdProvinceTop3DAO {
	void deleteAndInsertBatch(List<AdProvinceTop3> adProvinceTop3s);
}

package govind.dao;

import govind.domain.AdBlackList;

import java.util.List;

public interface IAdBlackListDAO {
	void insert(AdBlackList adBlackList);

	void insertBatch(List<AdBlackList> adBlackLists);
	List<AdBlackList> finall();
}

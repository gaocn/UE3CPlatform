package govind.dao;

import govind.domain.SessionAggrStat;
import govind.domain.SessionRandomExtract;

/**
 * Session随机抽取DAO
 */
public interface ISessionRandomExtractDAO {
	//插入随机抽取结果
	void insert(SessionRandomExtract sessionRandomExtract);
}

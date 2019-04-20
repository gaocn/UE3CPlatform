package govind.dao;

import govind.domain.SessionAggrStat;

/**
 * Session聚合统计模块DAO
 */
public interface ISessionAggrStatDAO {
	//插入聚合统计结果
	void insert(SessionAggrStat sessionAggrStat);
}

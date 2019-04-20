package govind.dao;

import govind.domain.Task;

/**
 * 任务管理DAO接口
 */
public interface ITaskDAO {
	/**
	 * 根据主键查询任务
	 * @param taskId 任务主键
	 * @return 任务
	 */
	Task findById(long taskId);

}

package govind.dao.impl;

import govind.dao.ITaskDAO;
import govind.domain.Task;
import govind.jdbc.JDBCHelper;

/**
 * 任务管理DAO实现类
 */
public class TaskDAOImpl implements ITaskDAO {

	/**
	 * 根据主键查找任务
	 * @param taskId 任务主键
	 * @return
	 */
	@Override
	public Task findById(long taskId) {
		Task task = new Task();
		String sql = "select * from task where task_id=?";
		Object[] params = new Object[]{taskId};
		JDBCHelper.getInstance().executeQuery(sql, params, rs -> {
			if (rs.next()) {
				long taskid = rs.getLong(1);
				String taskName = rs.getString(2);
				String createTime = rs.getString(3);
				String startTime = rs.getString(4);
				String finishTime = rs.getString(5);
				String taskType = rs.getString(6);
				String taskStatus = rs.getString(7);
				String taskParams = rs.getString(8);
				task.setTaskId(taskid);
				task.setTaskName(taskName);
				task.setCreateTime(createTime);
				task.setStartTime(startTime);
				task.setFinishTime(finishTime);
				task.setTaskType(taskType);
				task.setTaskStatus(taskStatus);
				task.setTaskParam(taskParams);
			}
		});
  		return task;
	}
}

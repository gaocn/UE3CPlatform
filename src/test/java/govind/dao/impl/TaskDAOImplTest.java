package govind.dao.impl;

import govind.dao.ITaskDAO;
import govind.domain.Task;
import org.junit.Test;

import static org.junit.Assert.*;

public class TaskDAOImplTest {

	@Test
	public void findById() {
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		Task task = taskDAO.findById(33);
		assertEquals(task.getTaskName(), "task1");
	}
}
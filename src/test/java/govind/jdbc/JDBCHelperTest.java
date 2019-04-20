package govind.jdbc;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

@Slf4j
public class JDBCHelperTest {

	@Test
	public void executeQuery() {
		String sql = "select * from task";
		JDBCHelper.getInstance().executeQuery(sql, null, rs -> {
			while (rs.next()) {
				String col = rs.getString("task_name");
				log.info("{}", col);
			}
		});
	}

	@Test
	public void insert() {
		String sql = "insert into task(task_id, task_name, create_time, start_time, finish_time, task_type, task_status, task_param) values (?,?,?,?,?,?,?,?)";
		Object[] params = {
				null,"task2", "2019-04-14 17:32:00", "2019-04-14 17:42:00", "2019-04-14 17:52:00",
				"2", "2", "{\"taskParm\":\"4\"}"
		};
		assertEquals(JDBCHelper.getInstance().executeUpdate(sql, params), 1);
	}

	@Test
	public void update(){
		String sql = "update task set task_name=? where task_id=?";
		Object[] params = {"update_task", 2};
		assertEquals(JDBCHelper.getInstance().executeUpdate(sql, params), 1);
	}
	@Test
	public void delete() {
		String sql = "delete from task where task_id = ?";
		Object[] params = {2};
		assertEquals(JDBCHelper.getInstance().executeUpdate(sql, params), 1);
	}

	@Test
	public void executeBatch() {
		String sql = "insert into task(task_id, task_name, create_time, start_time, finish_time, task_type, task_status, task_param) values (?,?,?,?,?,?,?,?)";
		List<Object[]> paramList = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			Object[] params = {
					null,"task2", "2019-04-14 17:32:00", "2019-04-14 17:42:00", "2019-04-14 17:52:00",
					"2", "2", "{\"taskParm\":\"4\"}"};
			params[1] = "task_" + (i + 1);
			paramList.add(params);
		}
		int[] rtn = new int[10];
		Arrays.fill(rtn, 1);
		assertTrue(Arrays.equals(rtn,JDBCHelper.getInstance().executeBatch(sql, paramList)));
	}
}
package govind.dao.impl;

import govind.dao.IPageConvertRateDAO;
import govind.domain.PageConvertRate;
import govind.jdbc.JDBCHelper;

public class PageConvertRateDAOImpl implements IPageConvertRateDAO {
	@Override
	public void insert(PageConvertRate pageConvertRate) {
		String sql = "insert into page_convert_rate values(?,?)";
		Object[] params = new Object[] {
				pageConvertRate.getTaskid(),
				pageConvertRate.getConvertRate()
		};

		JDBCHelper.getInstance().executeUpdate(sql, params);
	}
}

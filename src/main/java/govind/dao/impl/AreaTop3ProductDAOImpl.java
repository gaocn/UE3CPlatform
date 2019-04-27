package govind.dao.impl;

import govind.dao.IAreaTop3ProductDAO;
import govind.domain.AreaTop3Product;
import govind.jdbc.JDBCHelper;

public class AreaTop3ProductDAOImpl implements IAreaTop3ProductDAO {
	@Override
	public void insert(AreaTop3Product areaTop3Product) {
		String sql = "insert into area_top3_product values(?,?,?,?,?,?,?,?)";
		Object[] params = new Object[] {
				areaTop3Product.getTaskid(),
				areaTop3Product.getArea(),
				areaTop3Product.getAreaLevel(),
				areaTop3Product.getProductid(),
				areaTop3Product.getCityNames(),
				areaTop3Product.getClickCount(),
				areaTop3Product.getProductName(),
				areaTop3Product.getProductStatus()
		};
		JDBCHelper.getInstance().executeUpdate(sql, params);
	}
}

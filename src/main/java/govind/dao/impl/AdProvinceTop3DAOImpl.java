package govind.dao.impl;

import govind.dao.IAdProvinceTop3DAO;
import govind.domain.AdProvinceTop3;
import govind.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

public class AdProvinceTop3DAOImpl implements IAdProvinceTop3DAO {
	@Override
	public void deleteAndInsertBatch(List<AdProvinceTop3> adProvinceTop3s) {
		//1. 先收集去重后date province
		List<String> dateProvinces = new ArrayList<>();
		for (AdProvinceTop3 adProvinceTop3 : adProvinceTop3s) {
			String date = adProvinceTop3.getDate();
			String province = adProvinceTop3.getProvince();
			String key = date + "_" + province;
			if (!dateProvinces.contains(key)) {
				dateProvinces.add(key);
			}
		}
		//2. 根据去重后的date和province，批量删除
		String deleteSql = "DELETE FROM ad_province_top3 WHERE date=? AND province=?";
		List<Object[]> deleteParamList = new ArrayList<>();
		for (String toDelete : dateProvinces) {
			String[] dateProvince = toDelete.split("_");
			String date = dateProvince[0];
			String province = dateProvince[1];
			Object[] params = new Object[]{date, province};
			deleteParamList.add(params);
		}
		JDBCHelper.getInstance().executeBatch(deleteSql, deleteParamList);

		//3. 批量插入
		String insertSql = "INSERT INTO ad_province_top3 VALUES(?,?,?,?)";
		List<Object[]> insertParamList = new ArrayList<>();
		for (AdProvinceTop3 toInsert : adProvinceTop3s) {
			Object[] params = new Object[]{
					toInsert.getDate(),
					toInsert.getProvince(),
					toInsert.getAdid(),
					toInsert.getClickCount()
			};
			insertParamList.add(params);
		}
		JDBCHelper.getInstance().executeBatch(insertSql, insertParamList);
	}
}

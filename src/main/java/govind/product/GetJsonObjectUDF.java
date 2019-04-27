package govind.product;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.sql.api.java.UDF2;

/**
 * 从 {"product_status":1}中获取商品状态信息
 */
public class GetJsonObjectUDF implements UDF2<String, String, Integer> {
	@Override
	public Integer call(String s, String s2) throws Exception {
		JSONObject jsonObject = JSONObject.parseObject(s);
		String value = jsonObject.get(s2).toString();
		return Integer.valueOf(value);
	}
}

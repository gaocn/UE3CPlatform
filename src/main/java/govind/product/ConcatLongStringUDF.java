package govind.product;

import org.apache.spark.sql.api.java.UDF3;

/**
 * 根据指定分隔符将两个分隔符拼接起来
 */
public class ConcatLongStringUDF implements UDF3<Long, String, String, String> {
	@Override
	public String call(Long v1, String split, String v2) throws Exception {
		return String.valueOf(v1) + split + v2;
	}
}

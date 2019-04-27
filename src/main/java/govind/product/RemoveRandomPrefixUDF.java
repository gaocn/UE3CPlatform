package govind.product;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;

import java.util.Random;

/**
 * 去除添加的随机前缀
 * remove_random_prefix函数
 */
public class RemoveRandomPrefixUDF implements UDF1<String, String> {
	@Override
	public String call(String s) throws Exception {
		return s.split("_")[1];
	}
}

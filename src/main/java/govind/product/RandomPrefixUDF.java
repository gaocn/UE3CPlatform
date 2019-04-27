package govind.product;

import org.apache.spark.sql.api.java.UDF2;

import java.util.Random;

/**
 * random_prefix函数
 */
public class RandomPrefixUDF implements UDF2<String, Integer, String> {
	@Override
	public String call(String s, Integer num) throws Exception {
		Random rand = new Random();
		return rand.nextInt(num) + "_" + s;
	}
}

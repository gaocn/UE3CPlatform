package govind.util;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;

/**
 * 数字格式化类
 */
@Slf4j
public class NumberUtils {
	/**
	 * 格式化小数
	 * @param num 数字
	 * @param scale 保留几位小数
	 * @return 格式化后数字
	 */
	public static double formatDouble(double num, int scale) {
		BigDecimal decimal = new BigDecimal(num);
		return decimal.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue();
	}
}

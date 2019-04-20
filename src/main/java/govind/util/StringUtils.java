package govind.util;

/**
 * 字符串工具类
 */
public class StringUtils {
	/**
	 * 判断字符串是否为空
	 *
	 * @param str 字符串
	 * @return {@code true}不为空
	 */
	public static boolean isEmpty(String str) {
		return str == null || "".equals(str);
	}

	/**
	 * 判断字符串是否不为空
	 *
	 * @param str 字符串
	 * @return @code true}不为空
	 */
	public static boolean isNonEmpty(String str) {
		return str != null && !("".equals(str));
	}

	/**
	 * 截断字符串两侧的逗号
	 *
	 * @param str 字符串
	 * @return
	 */
	public static String trimComma(String str) {
		if (str.startsWith(",")) {
			str = str.substring(1);
		}
		if (str.endsWith(",")) {
			str = str.substring(0, str.length() - 1);
		}
		return str;
	}

	/**
	 * 补全两位数字
	 *
	 * @param str
	 * @return
	 */
	public static String fillWithZero(String str) {
		if (str.length() == 2) {
			return str;
		} else if (str.length() == 1) {
			return "0" + str;
		} else {
			throw new IllegalArgumentException(String.format("字符串[%s]长度大于2", str));
		}
	}

	/**
	 * 从拼接的字符串中提取字段
	 * 例如：str = K1=V1,K2=V2...
	 * {@code getFieldFromConcatString(str, ",", "K1") = "V2"}
	 *
	 * @param str       多个由”=“连接的Key-Value值
	 * @param delimiter 分割Key-Value值的分隔符
	 * @param field     要获取的Key-Value值的Key
	 * @return Key-Value的Value
	 */
	public static String getFieldFromConcatString(String str, String delimiter, String field) {
		String[] splits = str.split(delimiter);
		for (String tuple : splits) {
			if (tuple.split("=").length == 2) {
				String key = tuple.split("=")[0];
				String value = tuple.split("=")[1];
				if (field.equals(key)) {
					return value;
				}
			}
		}
		return null;
	}

	/**
	 * 从拼接的字符串中的字段设置值
	 *
	 * @param str           字符串
	 * @param delimiter     key-value的分隔符
	 * @param field         key字段名
	 * @param newFieldValue 新的key对应的值
	 * @return 新设置后的字符串
	 */
	public static String setFieldInConcatString(String str, String delimiter, String field, String newFieldValue) {
		String[] fields = str.split(delimiter);
		for (int i = 0; i < fields.length; i++) {
			String fieldName = fields[i].split("=")[0];
			if (field.equals(fieldName)) {
				String concatField = fieldName + "=" + newFieldValue;
				fields[i] = concatField;
				break;
			}
		}
		StringBuffer buffer = new StringBuffer();
		for (int i = 0; i < fields.length; i++) {
			buffer.append(fields[i]);
			if (i != fields.length - 1) {
				buffer.append("|");
			}
		}
		return buffer.toString();
	}
}


package govind.util;

import com.alibaba.fastjson.*;
import govind.conf.ConfigurationManager;
import govind.constant.Constants;
import lombok.extern.slf4j.Slf4j;

/**
 * 参数工具类
 */
@Slf4j
public class ParamUtils {
	/**
	 * 从命令行参数中提取任务id
	 * @param args 命令行参数
	 * @return 任务id
	 */
	public static Long getTaskIdFromArgs(String[] args, String taskType) {
		boolean isLocal = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if (isLocal) {
			return ConfigurationManager.getLong(taskType);
		} else {
			try {
				if (args != null && args.length > 0) {
					return Long.valueOf(args[0]);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	/**
	 * 从JSON对象中提取参数
	 * @param jsonObject json对象
	 * @param param 要抽取参数
	 * @return 参数值
	 */
	public static String getParam(JSONObject jsonObject, String param) {
		JSONArray array = jsonObject.getJSONArray(param);
		if (array != null && array.size() > 0) {
			return array.get(0).toString();
		}
		return null;
	}
}

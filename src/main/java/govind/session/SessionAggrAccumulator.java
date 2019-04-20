package govind.session;

import govind.constant.Constants;
import govind.util.ParamUtils;
import govind.util.StringUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import scala.Option;

/**
 * session聚合统计Accumulator
 * 自定义Accumulator可以使用自定义model(必须可序列化)然后基于这种数据格式实
 * 现复杂的分布式计算逻辑。
 */
public class SessionAggrAccumulator implements AccumulatorParam<String> {

	/**
	 * 用于数据初始化，这里所有范围区间的数量初始值都为0
	 * @param initialValue 各个范围区间统计量的拼接，采用k1=v1|k2=v2方式
	 *                     拼接
	 */
	@Override
	public String zero(String initialValue) {
		return Constants.SESSION_COUNT + "=0|"
				+ Constants.TIME_PERIOD_1s_3s + "=0|"
				+ Constants.TIME_PERIOD_4s_6s + "=0|"
				+ Constants.TIME_PERIOD_7s_9s + "=0|"
				+ Constants.TIME_PERIOD_10s_30s + "=0|"
				+ Constants.TIME_PERIOD_30s_60s + "=0|"
				+ Constants.TIME_PERIOD_1m_3m + "=0|"
				+ Constants.TIME_PERIOD_3m_10m + "=0|"
				+ Constants.TIME_PERIOD_10m_30m + "=0|"
				+ Constants.TIME_PERIOD_30m + "=0|"
				+ Constants.STEP_PERIOD_1_3 + "=0|"
				+ Constants.STEP_PERIOD_4_6 + "=0|"
				+ Constants.STEP_PERIOD_7_9 + "=0|"
				+ Constants.STEP_PERIOD_10_30 + "=0|"
				+ Constants.STEP_PERIOD_30_60 + "=0|"
				+ Constants.STEP_PERIOD_60 + "=0|";
	}

	/**
	 * addInPlace和addAccumulator可以认为是一样的，主要工作是：在v1中找到
	 * v2对应的value，累加1然后更新回到连接串中。
	 * @param v1 可能就是我们初始化的字符串
	 * @param v2 在遍历session时，判断出某个session对应的区间例如：
	 * 			 {@link Constants.TIME_PERIOD_1s_3s}
	 */
	@Override
	public String addInPlace(String v1, String v2) {
		return add(v1, v2);
	}

	@Override
	public String addAccumulator(String v1, String v2) {
		return add(v1, v2);
	}

	/**
	 * session统计计算逻辑
	 * @param v1 连接串
	 * @param v2 范围区间
	 * @return 更新以后的连接串
	 */
	private String add(String v1, String v2) {
		//校验，若v1为空直接返回v2
		if (StringUtils.isEmpty(v1)) {
			return v2;
		}
		//使用StringUtils工具从v1中提取v2对应值，并累加1
		String oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
		if (oldValue != null) {
			int newValue = Integer.parseInt(oldValue) + 1;
			//更新累加1后的新值
			return StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
		}
		return v1;
	}
}

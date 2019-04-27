package govind.product;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

/**
 * 组内拼接去重函数
 */
@Slf4j
public class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {
	/**
	 * 指定输入数据的字段与类型
	 */
	private StructType inputSchema = DataTypes.createStructType(Arrays.asList(
			DataTypes.createStructField("cityInfo", DataTypes.StringType, true)
	));

	/**
	 * 指定缓冲数据的字段与类型，在进行一个个拼接时需要维护的中间状态变量类型
	 */
	private StructType bufferSchema = DataTypes.createStructType(Arrays.asList(
			DataTypes.createStructField("bufferCityInfo", DataTypes.StringType, true)));
	/**
	 * 指定返回类型
	 */
	private DataType dataType = DataTypes.StringType;
	/**
	 * 指定是否是确定性的，一般为true
	 */
	private boolean deterministic = true;

	@Override
	public StructType inputSchema() {
		return inputSchema;
	}

	@Override
	public StructType bufferSchema() {
		return bufferSchema;
	}

	@Override
	public DataType dataType() {
		return dataType;
	}

	@Override
	public boolean deterministic() {
		return deterministic;
	}

	/**
	 * 初始化，可以指定一个初始的值
	 */
	@Override
	public void initialize(MutableAggregationBuffer buffer) {
		buffer.update(0, "");
	}

	/**
	 * 更新，一个一个将组内值传递进来，要实现拼接逻辑
	 * @param buffer
	 * @param input
	 */
	@Override
	public void update(MutableAggregationBuffer buffer, Row input) {
		//缓冲中的已经拼接过的城市信息
		String bufferCityInfo = buffer.getString(0);
		//刚刚传递进来的某个城市信息
		String cityInfo = input.getString(0);
		//要实现去重的逻辑，这里需要判断之前没有拼接过该信息
		if ("".equals(bufferCityInfo)) {
			bufferCityInfo += cityInfo;
		} else {
			// bufferCityInfo=1:北京 cityInfo=2:上海
			bufferCityInfo += "," + cityInfo;
		}
		//更新到缓冲的中间结果
		buffer.update(0, bufferCityInfo);
	}

	/**
	 * 合并，update操作时针对一个分组内的部分数据，在某个节点上发生的，但是可
	 * 能一个分组内的数据，会分布在多个节点上处理，此时就要用merge操作，将各个
	 * 节点上分布式拼接好的串，合并起来。
	 */
	@Override
	public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
		String bufferCityInfo1 = buffer1.getString(0);
		String bufferCityInfo2 = buffer2.getString(0);
		for (String cityInfo : bufferCityInfo2.split(",")) {
			if (!bufferCityInfo1.contains(cityInfo)) {
				if ("".equals(bufferCityInfo1)) {
					bufferCityInfo1 += cityInfo;
				} else {
					bufferCityInfo1 += "," + cityInfo;
				}
			}
		}
		buffer1.update(0, bufferCityInfo1);
	}

	/**
	 * 针对最终缓存的结果进行处理
	 */
	@Override
	public Object evaluate(Row buffer) {
		return buffer.getString(0);
	}
}

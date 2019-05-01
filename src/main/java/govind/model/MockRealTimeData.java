package govind.model;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.annotation.Nullable;
import java.util.*;

@Slf4j
public class MockRealTimeData {
	public static void main(String[] args) throws InterruptedException {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "node129:9092,node128:9092");
		props.setProperty("acks", "0");
		props.setProperty("retries", "0");
		props.setProperty("key.serializer", StringSerializer.class.getName());
		props.setProperty("value.serializer", StringSerializer.class.getName());
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		String topic = "AdLog";
		while (true) {
			String data = generateData();
			String randKey = UUID.randomUUID().toString();
			ProducerRecord<String, String> record = new ProducerRecord<>(topic, randKey, data);
			producer.send(record);
			Thread.sleep(1000);
			log.info("{}", record);
		}
	}
	private static final String[] provinces = {
			"北京", "上海", "宁夏","内蒙古","广西","安徽","黑龙江"
	};
	private static final String[][] cities = {
			{"北京"},
			{"上海"},
			{"银川","中卫"}, // "石嘴山","吴忠", "固原",
			{"呼和浩特", "包头"}, //, "乌海", "赤峰", "通辽"
			{"南宁","北海"},  //"柳州","桂林","梧州",
			{"合肥","亳州"}, //,"黄山","安庆","六安"
			{"哈尔滨","大庆","齐齐哈尔"} //,"佳木斯","鸡西"
	};
	/**
	 * 产生一条日志
	 * @return timestamp province city userid adid
	 */
	public static String generateData() {
		String timestamp = String.valueOf(System.currentTimeMillis());
		Random rand = new Random();
		int provinceId = rand.nextInt(provinces.length);
		String province = provinces[provinceId];
		String city = cities[provinceId][rand.nextInt(cities[provinceId].length)];
		long userid = rand.nextInt(20);
		long adid = rand.nextInt(20);

		String record = timestamp + " " + province + " " + city + " " + userid + " " + adid;
		return record;
	}

}

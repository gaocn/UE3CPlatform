package govind.ad;

import govind.conf.ConfigurationManager;
import govind.constant.Constants;
import govind.dao.*;
import govind.dao.impl.DAOFactory;
import govind.domain.*;
import govind.util.DateUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * 广告点击流量实时统计spark作业
 */
public class AdClickRealTimeStatAnalysis {
	public static void main(String[] args) {
		//1. 构建上下午
		SparkConf conf = new SparkConf()
				.setMaster("local[2]")
				.setAppName("AdClickRealTimeStatAnalysis");
		//streaming context上下文创建，指定实时处理Batch Duration一般
		// 是数秒到数十秒，很少会超过1分钟。
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		jssc.checkpoint("hdfs://node129:9000/checkpoint");

		//2. 创建数据源为Kafka的输入DStream(代表无界的数据集，数据源的抽象)
		//这里使用Kafka的Direct API，包括自适应调整每次接收数据量、transform exactly once semantics
		Map<String, String> kakfaParams = new HashMap<>();
		kakfaParams.put("bootstrap.servers", ConfigurationManager.getProperty(Constants.KAFKA_BOOTSTRAP_SERVERS));
		kakfaParams.put("bootstrap.servers", ConfigurationManager.getProperty(Constants.KAFKA_BOOTSTRAP_SERVERS));
		String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
		Set<String> topicSet = new HashSet<>(Arrays.asList(kafkaTopics.split(",")));

		JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(
				jssc,
				String.class, String.class,
				StringDecoder.class, StringDecoder.class,
				kakfaParams, topicSet);
		//根据动态还名单进行数据过滤
		JavaPairDStream<String, String> filterAdRealTimeLogDStream = filterByBlackList(adRealTimeLogDStream);
		//根据过滤数据计算当前每个用于每个广告的点击量
		JavaPairDStream<String, Long> dailyUserAdClickCountDStream = getAdClickCountDStream(filterAdRealTimeLogDStream);
		//持久化用于点击量到数据库中
		persistAdClickCount(dailyUserAdClickCountDStream);
		//动态生成黑名单
		generateBlackListDynamically(dailyUserAdClickCountDStream);
		//实时统计各省份广告点击流量
		JavaPairDStream<String, Long> adRealTimeStatDStream = calculateAdTrafficStat(filterAdRealTimeLogDStream);

		/**
		 * 业务功能二：实时统计每天各省份Top3热门广告
		 * rowsDStream中每一个batch rdd都代表最新的全量的每天各省各城市
		 * 各广告点击量。
		 */
		JavaDStream<Row> rowsDStream = adRealTimeStatDStream.transform((Function<JavaPairRDD<String, Long>, JavaRDD<Row>>) rdd -> {
			//数据格式：<yyyyMMdd_province_city_adid, click_count>
			//1. 需要计算每天各省的广告点击量
			//   数据格式：<yyyyMMdd_province_adid, click_count>
			JavaPairRDD<String, Long> dailyClickCountByProvince = rdd
					.mapToPair((PairFunction<Tuple2<String, Long>, String, Long>) tuple2 -> {
						String[] splits = tuple2._1.split("_");
						String date = splits[0];
						String province = splits[1];
						Long adid = Long.valueOf(splits[3]);
						return new Tuple2<>(date + "_" + province + "_" + adid, tuple2._2);
					})
					.reduceByKey((Function2<Long, Long, Long>) (v1, v2) -> v1 + v2);
			//将dailyClickCountByProvince转换为DataFrame并注册为临时表，
			// 然后使用Spark SQL的开窗函数获取到各省各广告点击Top3
			JavaRDD<Row> mapRDD = dailyClickCountByProvince.map((Function<Tuple2<String, Long>, Row>) v1 -> {
				String[] splits = v1._1.split("_");
				String dateKey = splits[0];
				String province = splits[1];
				Long adid = Long.valueOf(splits[2]);
				Long clickCount = v1._2;
				//日期转换为yyyy-MM-dd格式
				String date = DateUtils.formatDate(DateUtils.parseDateKey(dateKey));
				return RowFactory.create(date, province, adid, clickCount);
			});
			StructType structType = DataTypes.createStructType(Arrays.asList(
					DataTypes.createStructField("date", DataTypes.StringType, true),
					DataTypes.createStructField("province", DataTypes.StringType, true),
					DataTypes.createStructField("ad_id", DataTypes.LongType, true),
					DataTypes.createStructField("click_count", DataTypes.LongType, true)
			));
			//转换为DF并注册为临时表
			//需求启动Hive的远程metadata服务；hive --service metastore -p 9083
			HiveContext hiveContext = new HiveContext(rdd.context());
			DataFrame dataFrame = hiveContext.createDataFrame(mapRDD, structType);
			dataFrame.registerTempTable("tmp_daily_ad_click_count_by_province");
			//通过SQL实现Top3热门省份点击排名
			DataFrame provinceAdTop3DF = hiveContext.sql(
					"SELECT date, province, ad_id, click_count " +
							"FROM (" +
							"SELECT " +
							"date, " +
							"province, " +
							"ad_id, " +
							"click_count, " +
							"ROW_NUMBER() OVER (PARTITION BY province ORDER BY click_count DESC) rank " +
							"FROM tmp_daily_ad_click_count_by_province" +
							") T " +
							"WHERE rank <= 3");
			return provinceAdTop3DF.javaRDD();
		});
		//将rowsDStream中的Top3各省份广告点击量保存到MySQL数据库中。
		//更新数据时：1、若当天Top3数据不存在则插入；2、若存在先将已有数据删
		// 除再插入。
		rowsDStream.foreachRDD((VoidFunction<JavaRDD<Row>>) rdd -> {
			rdd.foreachPartition((VoidFunction<Iterator<Row>>) iter -> {
				List<AdProvinceTop3> adProvinceTop3s = new ArrayList<>();
				while (iter.hasNext()) {
					Row row = iter.next();
					String date = row.getString(0);
					String province = row.getString(1);
					long adid = row.getLong(2);
					long clickCount = row.getLong(3);
					AdProvinceTop3 adProvinceTop3 = new AdProvinceTop3();
					adProvinceTop3.setDate(date);
					adProvinceTop3.setProvince(province);
					adProvinceTop3.setAdid(adid);
					adProvinceTop3.setClickCount(clickCount);
					adProvinceTop3s.add(adProvinceTop3);
				}
				IAdProvinceTop3DAO adProvinceTop3DAO = DAOFactory.getAdProvinceTop3DAO();
				adProvinceTop3DAO.deleteAndInsertBatch(adProvinceTop3s);
			});
		});

		/**
		 * 业务功能三：实时统计每天每个广告在最近1小时的滑动窗口内的点击趋势
		 * （每分钟点击量）
		 * 效果是：每个广告最近1小时每分钟的点击量，便于可视化。
		 */
		//每个batch中的rdd都会映射为<yyyyMMddHHmm, 1L>格式
		JavaPairDStream<String, Long> aggRDD = adRealTimeLogDStream
				.mapToPair((PairFunction<Tuple2<String, String>, String, Long>) tuple2 -> {
					String[] splits = tuple2._2.split(" ");
					String timestamp = splits[0];
					//将时间戳转换为 yyyyMMddHHmm格式的时间
					String timeMinute = DateUtils.formatTimeMinute(new Date(Long.valueOf(timestamp)));
					long adid = Long.valueOf(splits[4]);
					return new Tuple2<>(timeMinute + "_" + adid, 1L);
				})
				//要计算1小时滑动窗口内的广告点击趋势，每次出来的新batch，都要获得最
				// 近1小时内的所有batch，然后根据key进行reduceBy操作，统计出来最近
				// 1小时内的各分钟各广告的点击次数。
				.reduceByKeyAndWindow(
						(Function2<Long, Long, Long>) (v1, v2) -> v1 + v2,
						// 时间窗口为1小时
						Durations.minutes(60),
						// 滑动窗口为10秒，每10秒钟做一次滑动窗，即每10秒钟把最近1小时
						// 的数据拿出来做一次reduceByKey的操作
						Durations.seconds(10));
		//最后结果格式为：<yyyyMMddHHmm_adid, count>，将结果持久化数据库
		//DAO操作：若库中存在则更新，若不存在则插入
		aggRDD.foreachRDD((VoidFunction<JavaPairRDD<String, Long>>) rdd -> {
			rdd.foreachPartition((VoidFunction<Iterator<Tuple2<String, Long>>>) iterator -> {
				List<AdClickTrend> adClickTrends = new ArrayList<>();
				while (iterator.hasNext()) {
					Tuple2<String, Long> tuple2 = iterator.next();
					String[] splits = tuple2._1.split("_");
					//yyyyMMddHHmm
					String dateMinute = splits[0];
					long adid = Long.valueOf(splits[1]);
					long clickCount = tuple2._2;

					String date= DateUtils.formatDate(DateUtils.parseDateKey(dateMinute.substring(0, 8)));
					String hour= dateMinute.substring(8, 10);
					String minute= dateMinute.substring(10);
					AdClickTrend adClickTrend = new AdClickTrend();
					adClickTrend.setDate(date);
					adClickTrend.setHour(hour);
					adClickTrend.setMinute(minute);
					adClickTrend.setAdid(adid);
					adClickTrend.setClickCount(clickCount);
					adClickTrends.add(adClickTrend);
				}

				IAdClickTrendDAO adClickTrendDAO = DAOFactory.getAdClickTrendDAO();
				adClickTrendDAO.insertAndUpdateBatch(adClickTrends);
			});
		});


		// 启动上下文、等待执行技术、关闭
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
	private static JavaPairDStream<String, Long> calculateAdTrafficStat(JavaPairDStream<String, String> filterAdRealTimeLogDStream) {
		/**
		 * 业务逻辑一、每天各省各城市广告的点击量
		 * 上述黑名单是广告实时系统中的基础应用，实际上我们要实现的业务不是黑
		 * 名单。要计算的广告点击量需要不断地更新到MySQL中，J2EE系统是提供
		 * 实时报表给用户查看到的，例如J2EE系统每隔几秒钟，就从MySQL中pull
		 * 一次最新数据，每次都可能不一样。
		 *
		 * 这里设计了几个维度：日期、省份、城市、广告，这样J2EE系统可以灵活的
		 * 为用户展示实时数据，同时允许用户根据查询范围(广告主、广告名称、广
		 * 告类型)显示用户感兴趣的广告的最新数据，这样就能够实现比较灵活的广告
		 * 点击流量查看功能。
		 * 原始数据：  date province city userid adid
		 * 转换为：  <date_province_city_adid count>
		 * 然后利用updateStateByKey算子更新状态，该算子会在Spark集群内存
		 * 中维护一份Key的全局状态。
		 *
		 * aggDStream中有每个batch rdd累加的各个key，每次计算出最新的值，
		 * 就在aggDStream中的每个batch rdd中反映出来。
		 */
		JavaPairDStream<String, Long> aggDStream = filterAdRealTimeLogDStream
				.mapToPair((PairFunction<Tuple2<String, String>, String, Long>) tuple2 -> {
					String[] splits = tuple2._2.split(" ");
					String timestamp = splits[0];
					Date date = new Date(Long.valueOf(timestamp));
					//日志格式：yyyyMMdd
					String dateKey = DateUtils.formatDateKey(date);
					String province = splits[1];
					String city = splits[2];
					String adid = splits[4];
					return new Tuple2<>(dateKey + "_" + province + "_" + city + "_" + adid, 1L);
				})
				.updateStateByKey((Function2<List<Long>, com.google.common.base.Optional<Long>, com.google.common.base.Optional<Long>>) (values, optional) -> {
					/**
					 * 对于每个Key都会调用一次这个方法
					 * 例如，一个batch中的rdd内容如下：
					 *   rdd1 <20190428_AnHui_BoZou_101, 1>
					 *   rdd2 <20190428_AnHui_BoZou_101, 2>
					 *   rdd3 <20190428_AnHui_BoZou_101, 3>
					 * 则values为[1, 2, 3]
					 */

					//1、根据Option判断之前这个Key是否有对应的状态
					long clickCount = 0L;
					//如果之前是存在状态的，则以之前的状态为起点进行值的累加
					if (optional.isPresent()) {
						clickCount = optional.get();
					}
					//values代表了，batch rdd中每个key对应的所有的值
					for (Long v : values) {
						clickCount += v;
					}
					return com.google.common.base.Optional.of(clickCount);
				});

		//将最新计算的结果插入到MySQL数据库中
		aggDStream.foreachRDD((VoidFunction<JavaPairRDD<String, Long>>) rdd -> {
			rdd.foreachPartition((VoidFunction<Iterator<Tuple2<String, Long>>>) iterator -> {
				List<AdStat> adStats = new ArrayList<>();
				while (iterator.hasNext()) {
					Tuple2<String, Long> tuple2 = iterator.next();
					String[] splits = tuple2._1.split("_");
					String date = splits[0];
					String province = splits[1];
					String city = splits[2];
					long adid = Long.valueOf(splits[3]);
					long clickCount = tuple2._2;

					AdStat adStat = new AdStat();
					adStat.setDate(date);
					adStat.setProvince(province);
					adStat.setCity(city);
					adStat.setAdid(adid);
					adStat.setClickCount(clickCount);
					adStats.add(adStat);
				}
				IAdStatDAO adStatDAO = DAOFactory.getAdStatDAO();
				//分两种情况：若数据库有则直接覆盖更新（注意不是累加），若没有插入
				adStatDAO.insertAndUpdateBatch(adStats);
			});
		});
		return aggDStream;
	}

	private static void generateBlackListDynamically(JavaPairDStream<String, Long> dailyUserAdClickCountDStream) {
		//6. 过滤出每个Batch中黑名单用户以生成动态黑名单
		/**
		 * 目前MySQL中已经有用户每天的广告点击量，只需要遍历每个batch中的所
		 * 有记录，对每天记录中的用户查询其当前的广告点击量，若超过100则认为
		 * 是黑名单用户，持久化到MySQL的数据库表中。
		 *
		 * 对哪个DStream进行遍历操作？dailyUserAdClickCountDStream是已
		 * 经聚合后的数据，格式为<yyyyMMdd_userid_adid, count>，比如一
		 * 个batch中有1万条数据，聚合后可能只有2千条，处理的数据量少。
		 */
		JavaPairDStream<String, Long> blackListDStream = dailyUserAdClickCountDStream.filter((Function<Tuple2<String, Long>, Boolean>) v1 -> {
			String[] splited = v1._1.split("_");
			//从yyyyMMdd转换为yyyy-MM-dd形式
			String date = DateUtils.formatDate(DateUtils.parseDateKey(splited[0]));
			long userid = Long.valueOf(splited[1]);
			long adid = Long.valueOf(splited[2]);

			IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
			int clickCount = adUserClickCountDAO.findClickCountByKeys(date, userid, adid);

			if (clickCount >= 100) {
				return true;
			}
			return false;
		});
		/**
		 * 在将黑名单用户持久化到数据库前，我们可以看到blackListDStream中
		 * 会存在重复的userid，因此需要先过滤。例如：
		 * yyyyMMdd_userid_adid
		 * 20190429_10001_10002  101
		 * 20190429_10001_10003  102
		 * 因此userid=10001就是重复的。
		 */
		JavaDStream<Long> uniqueUseridBlackListDStream = blackListDStream
				.map((Function<Tuple2<String, Long>, Long>) v1 -> {
					long userid = Long.valueOf(v1._1.split("_")[1]);
					return userid;
				})
				.transform((Function<JavaRDD<Long>, JavaRDD<Long>>) rdd -> rdd.distinct());
		//持久化黑名单到数据库
		uniqueUseridBlackListDStream.foreachRDD((Function<JavaRDD<Long>, Void>) rdd -> {
			rdd.foreachPartition((VoidFunction<Iterator<Long>>) iterator -> {
				List<AdBlackList> adBlackLists = new ArrayList<>();
				while (iterator.hasNext()) {
					AdBlackList adBlackList = new AdBlackList();
					adBlackList.setUserid(iterator.next());
					adBlackLists.add(adBlackList);
				}
				IAdBlackListDAO adBlackListDAO = DAOFactory.getAdBlackListDAO();
				adBlackListDAO.insertBatch(adBlackLists);
			});
			return null;
		});
	}

	private static void persistAdClickCount(JavaPairDStream<String, Long> dailyUserAdClickCountDStream) {
		//5. 用户点击量写入数据库
		dailyUserAdClickCountDStream.foreachRDD((VoidFunction<JavaPairRDD<String, Long>>) rdd -> {
			//对于每个分区数据获取一次连接对象进行批量插入，每次从连接池获取，
			// 因此性能已经优化到最大
			rdd.foreachPartition((VoidFunction<Iterator<Tuple2<String, Long>>>) iterator -> {
				List<AdUserClickCount> adUserClickCounts = new ArrayList<>();
				while (iterator.hasNext()) {
					Tuple2<String, Long> tuple = iterator.next();
					String[] keySplited = tuple._1.split("_");
					//转换为格式：yyyy-MM-dd
					String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));
					long userid = Long.valueOf(keySplited[1]);
					long adid = Long.valueOf(keySplited[2]);
					long clickCount = tuple._2;

					AdUserClickCount adUserClickCount = new AdUserClickCount();
					adUserClickCount.setDate(date);
					adUserClickCount.setUserid(userid);
					adUserClickCount.setAdid(adid);
					adUserClickCount.setClickCount(clickCount);
					adUserClickCounts.add(adUserClickCount);
				}
				IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
				adUserClickCountDAO.updateBatch(adUserClickCounts);
			});
		});
	}

	private static JavaPairDStream<String, Long> getAdClickCountDStream(JavaPairDStream<String, String> filterAdRealTimeLogDStream) {
		//3. 计算出每个5秒内的数据中，每天每个用户对某个广告的点击量
		//返回数据格式为：<yyyyMMdd_ueserid_adid, 1L>
		JavaPairDStream<String, Long> dailyUserAdClickDStream = filterAdRealTimeLogDStream.mapToPair((PairFunction<Tuple2<String, String>, String, Long>) tuple2 -> {
			//一条实时日志，按照空格分割：timestamp province city userid adid
			String log = tuple2._2;
			String[] logSplited = log.split(" ");
			//提取日期
			String timestamp = logSplited[0];
			Date date = new Date(Long.valueOf(timestamp));
			//日志格式：yyyyMMdd
			String dateKey = DateUtils.formatDateKey(date);

			long userid = Long.valueOf(logSplited[3]);
			long adid = Long.valueOf(logSplited[4]);
			//拼接Key，某天某用户某个广告
			String key = dateKey + "_" + userid + "_" + adid;
			return new Tuple2<>(key, 1L);
		});

		//4. 针对处理后的日志格式，计算当前batch中每天每个用户每个广告的点击量
		//获取到每5s batch中，当天每个用户对每支广告的点击量
		return dailyUserAdClickDStream.reduceByKey((Function2<Long, Long, Long>) (v1, v2) -> v1 + v2);
	}

	private static JavaPairDStream<String, String> filterByBlackList(JavaPairInputDStream<String, String> adRealTimeLogDStream) {
		//7. 基于动态黑名单进行点击行为过滤
		/**
		 * 对于接收到原始的用户点击行为日志，根据MySQL的动态黑名单进行实时黑
		 * 名单过滤(直接过滤掉)，这里需要使用transform算子。
		 *
		 * 对代码进行重新组织：
		 * 	(1)、先进行黑名单过滤；
		 * 	(2)、再根据过滤后的日志动态生成/更新黑名单
		 */
		return adRealTimeLogDStream
				.transformToPair((Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>) rdd -> {
					//从MySQL中查询所有黑名单用户，将其转换为一个RDD，因此
					// 需要添加一个DAO方法获取所有黑名单用户列表。
					IAdBlackListDAO adBlackListDAO = DAOFactory.getAdBlackListDAO();
					List<AdBlackList> adBlackLists = adBlackListDAO.finall();
					List<Tuple2<Long, Boolean>> tuple2s = new ArrayList<>();
					adBlackLists.forEach(adBlackList -> {
						tuple2s.add(new Tuple2<>(adBlackList.getUserid(), true));
					});
					JavaSparkContext jsc = new JavaSparkContext(rdd.context());
					JavaPairRDD<Long, Boolean> blackListRDD = jsc.parallelizePairs(tuple2s);

					//将原始数据RDD映射为 <userid, str1|str2>
					JavaPairRDD<Long, Tuple2<String, String>> mappedRDD = rdd
							.mapToPair((PairFunction<Tuple2<String, String>, Long, Tuple2<String, String>>) tuple2 -> {
								//一条实时日志，按照空格分割：timestamp province city userid adid
								long userid = Long.valueOf(tuple2._2.split(" ")[3]);
								return new Tuple2<>(userid, tuple2);
							});

					//将原始日志RDD与黑名单RDD进行左外连接，若原始日志的
					// userid没有在对应的黑名单中，则join不到，因此用左
					// 外连接，若用inner join则会出现join不到的数据丢失。
					JavaPairRDD<String, String> resultRDD = mappedRDD.leftOuterJoin(blackListRDD)
							.filter((Function<Tuple2<Long, Tuple2<Tuple2<String, String>, com.google.common.base.Optional<Boolean>>>, Boolean>) v1 -> {
								com.google.common.base.Optional<Boolean> inBlackList = v1._2._2;
								//这个值存在则过滤掉
								if (inBlackList.isPresent() && inBlackList.get()) {
									return false;
								}
								//若join之后的值不存在，则保留下来该条数据
								return true;
							})
							//映射为原始数据类型
							.mapToPair((PairFunction<Tuple2<Long, Tuple2<Tuple2<String, String>, com.google.common.base.Optional<Boolean>>>, String, String>) tuple2 -> tuple2._2._1);

					return resultRDD;
				});
	}
}

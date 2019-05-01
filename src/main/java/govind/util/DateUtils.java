package govind.util;

import lombok.extern.slf4j.Slf4j;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 日期时间工具类
 */
@Slf4j
public class DateUtils {

	public static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
	public static final SimpleDateFormat DATEKEY_FORMAT = new SimpleDateFormat("yyyyMMdd");

	/**
	 * 判断第一个时间是否在第二个时间之前
	 * @param time1 第一个时间
	 * @param time2 第二个时间
	 * @return {@code true} date1在date2之前
	 */
	public static boolean before(String time1, String time2) {
		try {
			Date dateTime1 = TIME_FORMAT.parse(time1);
			Date dateTime2 = TIME_FORMAT.parse(time2);
			return dateTime1.before(dateTime2);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 判断第一个时间是否在第二个时间之后
	 * @param time1 第一个时间
	 * @param time2 第二个时间
	 * @return {@code true} date1在date2之后
	 */
	public static boolean after(String time1, String time2) {
		try {
			Date dateTime1 = TIME_FORMAT.parse(time1);
			Date dateTime2 = TIME_FORMAT.parse(time2);
			return dateTime1.after(dateTime2);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 计算第一个时间与第二个时间的差值，单位为秒
	 * @param time1 第一个时间
	 * @param time2 第二个时间
	 * @return 差值
	 */
	public static int minus(String time1, String time2) {
		try {
			Date dateTime1 = TIME_FORMAT.parse(time1);
			Date dateTime2 = TIME_FORMAT.parse(time2);
			long millisecond = dateTime1.getTime() - dateTime2.getTime();
			return Integer.valueOf(String.valueOf(millisecond / 100));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return 0;
	}

	/**
	 * 获取年月日和小时
	 * @param datetime 时间 yyyy-MM-dd HH:mm:ss
	 * @return yyyy-MM-dd_HH
	 */
	public static String getDateHour(String datetime) {
		String date = datetime.split(" ")[0];
		String hourMinuteSecond = datetime.split(" ")[1];
		String hour = hourMinuteSecond.split(":")[0];
		return date + "_" + hour;
	}

	/**
	 * 获取当前日期
	 * @return 当天日期
	 */
	public static String getTodayDate() {
		return DATE_FORMAT.format(new Date());
	}

	/**
	 * 获取昨天的日期  yyyy-MM-dd
	 * @return 昨天日期
	 */
	public static String getYesterdayDate() {
		Calendar instance = Calendar.getInstance();
		instance.setTime(new Date());
		instance.add(Calendar.DAY_OF_YEAR, -1);
		Date date = instance.getTime();
		return DATE_FORMAT.format(date);
	}

	/**
	 * 格式化日期
	 * @param date Date对象
	 * @return yyyy-MM-dd
	 */
	public static String formatDate(Date date) {
		return DATE_FORMAT.format(date);
	}

	public static String formatDateKey(Date date) {
		return DATEKEY_FORMAT.format(date);
	}

	public static Date parseDateKey(String dateKey) {
		try {
			return DATEKEY_FORMAT.parse(dateKey);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 格式化时间
	 * @param date Date对象
	 * @return yyyy-MM-dd HH:mm:ss
	 */
	public static String formatTime(Date date) {
		return TIME_FORMAT.format(date);
	}

	/**
	 * 解析时间字符串
	 * @param time 时间字符串
	 * @return
	 */
	public static Date parseTime(String time) {
		try {
			return TIME_FORMAT.parse(time);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 格式化时间，保留到分钟级别
	 * @return yyyyMMddHHmm
	 */
	public static String formatTimeMinute(Date date) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		return sdf.format(date);
	}
}

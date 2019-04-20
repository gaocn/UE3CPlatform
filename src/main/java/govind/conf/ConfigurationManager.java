package govind.conf;

import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 * 1、配置管理组件可以简单，对于简单组件来说，开发一个类，可以在第一次访问它的
 * 时候，就从对应的properties文件中读取配置项，并提供外界获取某个配置key对应
 * 的value的方法。
 * 2、如果是而别复杂的配置管理组件，那么可能需要使用一些软件设计中的设计模式，
 * 比如单例模式、解释器模式，可能需要管理多个不同的properties，甚至是xml类型
 * 的配置文件。
 */
public class ConfigurationManager {
	//使用private修饰为了避免外界代码不小心错误更新了Properties文件中某个key
	//对应的value，从而导致整个程序的状态错误，乃至崩溃。
	private static Properties props = new Properties();
	/**
	 * 静态代码块
	 * Java中每一个类第一次使用时机会被JVM中的类加载器，去从磁盘上的.class
	 * 文件中加载出来，然后为每个类都会构建一个Class对象，就代表这个类。
	 *
	 * 每个类第一次加载是都会进行自身初始化，在类初始化时会执行类中static模块
	 * 中静态代码，因此类第一次使用时会加载，加载后就会初始化类，初始化类时会
	 * 执行类的静态代码块。在静态代码中的另一个好处：类的初始化在整个JVM生命
	 * 周期内有且仅有一次，即配置文件只会被会加载一次，以后重复使用。
	 */
	static {
		try {
			InputStream in = ConfigurationManager.class.getClassLoader()
					.getResourceAsStream("my.properties");
			//加载配置项
			props.load(in);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取指定key对应的value
	 * @param key
	 * @return value
	 */
	public static String getProperty(String key) {
		return props.getProperty(key);
	}

	/**
	 * 获取整型类型的配置项
	 * @param key
	 * @return
	 */
	public static Integer getInteger(String key) {
		String value = getProperty(key);
		try {
			return Integer.valueOf(value);
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}
		return 0;
	}

	public static boolean getBoolean(String key) {
		String value =  getProperty(key);
		try {
			return Boolean.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
}

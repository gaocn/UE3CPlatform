package govind;

public class SingletonTest {
	public static SingletonTest instance;
	private SingletonTest(){}
	/**
	 * public  static synchronized SingletonTest getInstance()的问题
	 * 当第一次创建单例对象时没有问题，但是当单例对象已经创建完成，由于同步关键
	 * 字导致该方法在多线程访问时会被阻塞保证顺序执行，由于已创建的实例是可以
	 * 被并发读取的，这就导致应用程序多线程等待，导致执行效率降低。
	 *
	 * 下面采用是的：Double-Check方法实现单例模式
	 */
	public  static SingletonTest getInstance() {
		if (instance == null) {
			synchronized (SingletonTest.class) {
				if (instance == null) {
					instance = new SingletonTest();
				}
			}
		}
		return instance;
	}
	/**
	 * 内部类和匿名内部类
	 * 外部类：平时最常见的普通的类，以.java结尾
	 * 内部类：包含在外部类中的类，有两种：静态内部类和非静态内部类，两者的主
	 * 要区别是:
	 * 1、内部原理，静态内部类属于外部类的类成员，是一种静态成员；而非静态内部
	 * 类属于外部类的实例对象的一个实例成员，即每个非静态内部类是属于外部类的
	 * 每个实例的，创建非静态内部类的实例后，非静态内类实例是必须跟一个外部类
	 * 实例进行关联和有寄存关系的。
	 * 2、创建方式，创建静态内部类的实例时，直接使用"外部类.内部类()"方式，比
	 * 如new School.Teacher()；而创建非静态内部类必须要创建一个外部类的实
	 * 例然后通过外部类实例创建内部类实例比如new School().Teacher().
	 *
	 * {{
	 *     	public class School{
	 * 			static class Teacher{}
	 * 		}
	 * 		public class School{
	 * 			class Teacher{}
	 *    	}
	 * }}
	 *
	 * 匿名内部类的使用场景
	 * 通常来说就是一个内部类，只创建一次、使用一次且以后不再使用，通常在一个
	 * 方法内部创建匿名内部类。
	 */
}

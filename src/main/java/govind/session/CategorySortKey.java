package govind.session;

import scala.math.Ordered;
import java.io.Serializable;

/**
 * 自定义二次排序Key，封装需要进行排序的几个字段，实现scala.math.Ordered[T]
 * 接口，复写其中的大于、大于等于、小于、小于等于函数。
 * 注意：自定义二次排序Key要实现序列化接口，否则会报错！！！
 */
public class CategorySortKey implements Ordered<CategorySortKey>, Serializable {
	private long clickCount;
	private long orderCount;
	private long payCount;

	@Override
	public boolean $greater(CategorySortKey that) {
		if (this.clickCount > that.clickCount) {
			return true;
		} else if (clickCount == that.clickCount
				&& orderCount > that.orderCount) {
			return true;
		} else if (clickCount == that.clickCount
				&& orderCount == that.orderCount
				&& payCount < that.payCount) {
			return true;
		}
		return false;
	}

	@Override
	public boolean $greater$eq(CategorySortKey that) {
		if ($greater(that)) {
			return true;
		} else if (clickCount == that.clickCount
				&& orderCount == that.orderCount
				&& payCount == that.payCount) {
			return true;
		}
		return false;
	}

	@Override
	public boolean $less(CategorySortKey that) {
		if (this.clickCount < that.clickCount) {
			return true;
		} else if (clickCount == that.clickCount
				&& orderCount < that.orderCount) {
			return true;
		} else if (clickCount == that.clickCount
				&& orderCount == that.orderCount
				&& payCount < that.payCount) {
			return true;
		}
		return false;
	}

	@Override
	public boolean $less$eq(CategorySortKey that) {
		if ($less(that)) {
			return true;
		} else if (clickCount == that.clickCount
				&& orderCount == that.orderCount
				&& payCount == that.payCount) {
			return true;
		}
		return false;
	}

	@Override
	public int compare(CategorySortKey that) {
		if (clickCount - that.clickCount != 0) {
			return (int) (clickCount - that.clickCount);
		} else if (orderCount - that.orderCount != 0) {
			return (int) (orderCount - that.orderCount);
		} else if (payCount - that.payCount != 0) {
			return (int) (payCount - that.payCount);
		}
		return 0;
	}

	@Override
	public int compareTo(CategorySortKey that) {
		if (clickCount - that.clickCount != 0) {
			return (int) (clickCount - that.clickCount);
		} else if (orderCount - that.orderCount != 0) {
			return (int) (orderCount - that.orderCount);
		} else if (payCount - that.payCount != 0) {
			return (int) (payCount - that.payCount);
		}
		return 0;
	}

	public long getClickCount() {
		return clickCount;
	}

	public void setClickCount(long clickCount) {
		this.clickCount = clickCount;
	}

	public long getOrderCount() {
		return orderCount;
	}

	public void setOrderCount(long orderCount) {
		this.orderCount = orderCount;
	}

	public long getPayCount() {
		return payCount;
	}

	public void setPayCount(long payCount) {
		this.payCount = payCount;
	}
}

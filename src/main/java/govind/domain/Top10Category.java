package govind.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Top10热门品类
 */
@Getter
@Setter
@NoArgsConstructor
public class Top10Category {
	private long taskId;
	private long categoryId;
	private long clickCount;
	private long orderCount;
	private long payCount;
}

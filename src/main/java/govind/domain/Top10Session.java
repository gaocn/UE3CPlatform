package govind.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Top10 活跃session
 */
@Getter
@Setter
@NoArgsConstructor
public class Top10Session {
	private long taskId;
	private long categoryId;
	private String sessionId;
	private long clickCount;
}

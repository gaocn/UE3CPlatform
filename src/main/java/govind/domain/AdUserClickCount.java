package govind.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class AdUserClickCount {
	private String date;
	private long userid;
	private long adid;
	private long clickCount;
}

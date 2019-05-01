package govind.domain;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class AdClickTrend {
	private String date;
	private String hour;
	private String minute;
	private long adid;
	private long clickCount;
}

package govind.domain;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class AdStat {
	private String date;
	private String province;
	private String city;
	private long adid;
	private long clickCount;

}

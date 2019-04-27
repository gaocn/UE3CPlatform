package govind.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class AreaTop3Product {
	private long taskid;
	private String area;
	private String areaLevel;
	private long productid;
	private String cityNames;
	private long clickCount;
	private String productName;
	private String productStatus;
}

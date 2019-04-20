package govind.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@NoArgsConstructor
public class SessionDetail {
	private long taskid;
	private long userid;
	private String sessionid;
	private long pageid;
	private String actionTime;
	private String searchKeywords;
	private long clickCategoryId;
	private long clickProductId;
	private String orderCategoryIds;
	private String orderProductIds;
	private String payCategoryIds;
	private String payProductIds;
}

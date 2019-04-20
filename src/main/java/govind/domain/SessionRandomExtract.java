package govind.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * 随机抽取的session
 */
@Setter
@Getter
@NoArgsConstructor
public class SessionRandomExtract {
	private long taskid;
	private String sessionid;
	private String startTime;
	private String searchKeyWords;
	private String clickCategoryIds;
}

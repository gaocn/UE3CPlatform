package govind.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@ToString
public class Task implements Serializable {
	private long taskId;
	private String taskName;
	private String createTime;
	private String startTime;
	private String finishTime;
	private String taskType;
	private String taskStatus;
	private String taskParam;
}

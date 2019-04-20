package govind.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Session聚合统计表对应的实体类
 */
@Getter
@Setter
@NoArgsConstructor
public class SessionAggrStat {
	private long taskid;
	private long sessionCount;
	double visiLength1s3sRation;
	double visiLength4s6sRation;
	double visiLength7s9sRation;
	double visiLength10s30sRation;
	double visiLength30s60sRation;
	double visiLength1m3mRation;
	double visiLength3m10mRation;
	double visiLength10m30mRation;
	double visiLength30mRation;
	double stepLength13Ration;
	double stepLength46Ration;
	double stepLength79Ration;
	double stepLength1030Ration;
	double stepLength3060Ration;
	double stepLength60Ration;
}

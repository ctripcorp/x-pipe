package com.ctrip.xpipe.spring;




import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author marsqing
 *
 *         May 26, 2016 6:34:49 PM
 */
public class AbstractController {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

	protected Joiner joiner = Joiner.on(",");

	public static final String API_PREFIX = "/api";
	public static final String META_API_TYPE = "MetaUpdate";

	public static final String CLUSTER_NAME_PATH_VARIABLE = "{clusterName:.+}";
	public static final String REGION_NAME_PATH_VARIABLE = "{regionName:.+}";
	public static final String AZ_NAME_PATH_VARIABLE = "{azName:.+}";
	public static final String SHARD_NAME_PATH_VARIABLE = "{shardName:.+}";
	public static final String CLUSTER_ID_PATH_VARIABLE = "{clusterId:.+}";
	public static final String SHARD_ID_PATH_VARIABLE = "{shardId:.+}";
	public static final String IP_ADDRESS_VARIABLE = "{ipAddress:.+}";

}

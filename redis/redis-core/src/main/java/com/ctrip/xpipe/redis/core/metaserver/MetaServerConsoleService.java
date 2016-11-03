package com.ctrip.xpipe.redis.core.metaserver;


import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;

/**
 * used for console
 * @author wenchao.meng
 *
 * Aug 2, 2016
 */
public interface MetaServerConsoleService extends MetaServerService{
	
	public static final String PATH_CLUSTER_CHANGE = "/clusterchange/{clusterId}";

	void clusterAdded(String clusterId, ClusterMeta clusterMeta);

	void clusterModified(String clusterId, ClusterMeta clusterMeta);

	void clusterDeleted(String clusterId);

	/**
	 * @return
	 */
	DcMeta getDynamicInfo();
}

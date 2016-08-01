package com.ctrip.xpipe.redis.meta.server.cluster;

/**
 * @author wenchao.meng
 *
 * Jul 25, 2016
 */
public interface ClusterArranger {
	
	void onServerAdded(ClusterServer clusterServer);
	
	void onServerRemoved(ClusterServer clusterServer);
	
	void onServerChanged(ClusterServer oldClusterServer, ClusterServer newClusterServer);

}

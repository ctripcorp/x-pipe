package com.ctrip.xpipe.redis.meta.server.cluster;

/**
 * @author wenchao.meng
 *
 * Jul 25, 2016
 */
public interface ClusterArranger {
	
	void onServerAdded(int clusterServer);
	
	void onServerRemoved(int clusterServer);
	
	void onServerChanged(ClusterServer oldClusterServer, ClusterServer newClusterServer);

	void moveSlot(int slotId, int fromServer, int toServer);
	
}

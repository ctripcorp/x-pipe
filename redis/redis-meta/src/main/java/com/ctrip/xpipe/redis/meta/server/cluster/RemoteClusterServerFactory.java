package com.ctrip.xpipe.redis.meta.server.cluster;

/**
 * @author wenchao.meng
 *
 * Jul 23, 2016
 */
public interface RemoteClusterServerFactory<T extends ClusterServer> {
	
	T  createClusterServer(int serverId, ClusterServerInfo clusterServerInfo);
	
}

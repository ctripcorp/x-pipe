package com.ctrip.xpipe.redis.meta.server.cluster;

import java.util.Map;
import java.util.Set;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.observer.Observable;

/**
 * @author wenchao.meng
 *
 * Jul 22, 2016
 */
public interface ClusterServers<T extends ClusterServer> extends Observable, Lifecycle{
	
	T currentClusterServer();
	
	T  getClusterServer(int serverId);
	
	Set<T> allClusterServers();
	
	void refresh() throws ClusterException;
	
	boolean exist(int serverId);

	Map<Integer, ClusterServerInfo> allClusterServerInfos();
}

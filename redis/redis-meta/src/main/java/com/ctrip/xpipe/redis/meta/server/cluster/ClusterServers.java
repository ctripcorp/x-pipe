package com.ctrip.xpipe.redis.meta.server.cluster;

import java.util.Set;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.observer.Observable;

/**
 * @author wenchao.meng
 *
 * Jul 22, 2016
 */
public interface ClusterServers extends Observable, Lifecycle{
	
	ClusterServer currentClusterServer();
	
	ClusterServer  getClusterServer(int serverId);
	
	Set<ClusterServer> allClusterServers();
	
	void refresh() throws Exception;
	
	boolean exist(int serverId);

}

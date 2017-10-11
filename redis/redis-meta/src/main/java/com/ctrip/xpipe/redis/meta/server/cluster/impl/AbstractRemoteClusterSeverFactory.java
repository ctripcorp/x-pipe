package com.ctrip.xpipe.redis.meta.server.cluster.impl;


import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServerInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.RemoteClusterServerFactory;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author wenchao.meng
 *
 * Jul 23, 2016
 */
public abstract class AbstractRemoteClusterSeverFactory<T extends ClusterServer> implements RemoteClusterServerFactory<T>{

	@Autowired
	private MetaServerConfig config;
	
	@Autowired
	protected T currentClusterServer;

	@Override
	public T createClusterServer(int serverId, ClusterServerInfo clusterServerInfo) {
		
		if(serverId == config.getMetaServerId()){
			return currentClusterServer;
		}
		return doCreateRemoteServer(serverId, clusterServerInfo);
	}

	protected abstract T doCreateRemoteServer(int serverId, ClusterServerInfo clusterServerInfo);
}

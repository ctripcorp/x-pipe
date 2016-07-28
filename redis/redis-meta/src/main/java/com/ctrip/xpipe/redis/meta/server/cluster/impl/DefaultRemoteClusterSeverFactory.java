package com.ctrip.xpipe.redis.meta.server.cluster.impl;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServerInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.RemoteClusterServerFactory;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;

/**
 * @author wenchao.meng
 *
 * Jul 23, 2016
 */
@Component
public class DefaultRemoteClusterSeverFactory implements RemoteClusterServerFactory{

	@Autowired
	private MetaServerConfig config;
	
	@Autowired
	private CurrentClusterServer currentClusterServer;

	@Override
	public ClusterServer createClusterServer(int serverId, ClusterServerInfo clusterServerInfo) {
		
		if(serverId == config.getMetaServerId()){
			return currentClusterServer;
		}
		return new RemoteClusterServer(serverId, clusterServerInfo);
	}
}

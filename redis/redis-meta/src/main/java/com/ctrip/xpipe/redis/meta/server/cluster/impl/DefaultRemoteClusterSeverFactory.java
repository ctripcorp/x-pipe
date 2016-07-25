package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import org.springframework.stereotype.Component;

import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServerInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.RemoteClusterServerFactory;

/**
 * @author wenchao.meng
 *
 * Jul 23, 2016
 */
@Component
public class DefaultRemoteClusterSeverFactory implements RemoteClusterServerFactory{

	@Override
	public ClusterServer createClusterServer(int serverId, ClusterServerInfo clusterServerInfo) {
		return new RemoteClusterServer(serverId, clusterServerInfo);
	}
	
	class RemoteClusterServer implements ClusterServer{
		
		private int serverId;
		private ClusterServerInfo clusterServerInfo;
		
		public RemoteClusterServer(int serverId, ClusterServerInfo clusterServerInfo) {
			
			this.serverId = serverId;
			this.clusterServerInfo = clusterServerInfo;
		}

		@Override
		public int getServerId() {
			return this.serverId;
		}

		@Override
		public ClusterServerInfo getClusterInfo() {
			return this.clusterServerInfo;
		}
		
	}
}

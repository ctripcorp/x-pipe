package com.ctrip.xpipe.redis.meta.server.impl;


import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServerInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.AbstractRemoteClusterSeverFactory;
import org.springframework.stereotype.Component;

/**
 * @author wenchao.meng
 *
 * Aug 3, 2016
 */
@Component
public class DefaultRemoteMetaServerFactory extends AbstractRemoteClusterSeverFactory<MetaServer>{
	

 	@Override
	protected MetaServer doCreateRemoteServer(int serverId, ClusterServerInfo clusterServerInfo) {
 		
		return new RemoteMetaServer(currentClusterServer.getServerId(), serverId, clusterServerInfo);
	}

}

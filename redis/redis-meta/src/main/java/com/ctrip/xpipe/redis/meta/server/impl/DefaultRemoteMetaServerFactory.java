package com.ctrip.xpipe.redis.meta.server.impl;


import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServerInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.AbstractRemoteClusterSeverFactory;
import com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.annotation.Resource;
import java.util.concurrent.ExecutorService;

/**
 * @author wenchao.meng
 *
 * Aug 3, 2016
 */
@Component
public class DefaultRemoteMetaServerFactory extends AbstractRemoteClusterSeverFactory<MetaServer>{

	@Resource(name = MetaServerContextConfig.GLOBAL_EXECUTOR)
	private ExecutorService executors;

 	@Override
	protected MetaServer doCreateRemoteServer(int serverId, ClusterServerInfo clusterServerInfo) {
 		
		return new RemoteMetaServer(currentClusterServer.getServerId(), serverId, clusterServerInfo, executors);
	}

}

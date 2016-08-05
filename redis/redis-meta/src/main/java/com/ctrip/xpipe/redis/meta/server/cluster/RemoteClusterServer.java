package com.ctrip.xpipe.redis.meta.server.cluster;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;

/**
 * @author wenchao.meng
 *
 * Jul 25, 2016
 */
public interface RemoteClusterServer extends ClusterServer, Lifecycle{
	
	int getCurrentServerId();
}

package com.ctrip.xpipe.redis.meta.server.cluster;

import java.util.Set;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;

/**
 * @author wenchao.meng
 *
 * Jul 25, 2016
 */
public interface CurrentClusterServer extends ClusterServer, Lifecycle{
	
	int ORDER = SlotManager.ORDER + 1;
	
	Set<Integer>  slots();
	
	boolean isLeader();

	boolean hasKey(Object key);
	
}

package com.ctrip.xpipe.redis.meta.server.cluster;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;

import java.util.Set;

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

package com.ctrip.xpipe.redis.meta.server.keeper;

import java.util.Set;

import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;

/**
 * @author wenchao.meng
 *
 * Aug 5, 2016
 */
public interface DynamicStateManager extends Observer{
	
	void add(ClusterMeta clusterMeta);

	void remove(String clusterId);
	
	void removeBySlot(int slotId);
	
	Set<String> allClusters();
	
	void ping(KeeperInstanceMeta keeperInstanceMeta);
	
	
}

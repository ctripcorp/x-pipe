package com.ctrip.xpipe.redis.meta.server.keeper;

import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;

import java.util.Set;

/**
 * @author wenchao.meng
 *
 * Aug 5, 2016
 */
public interface DynamicStateManager extends Observer{
	
	void add(ClusterMeta clusterMeta);

	void remove(Long clusterDbId);
	
	void removeBySlot(int slotId);
	
	Set<Long> allClusters();
	
	void ping(KeeperInstanceMeta keeperInstanceMeta);
	
	
}

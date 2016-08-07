package com.ctrip.xpipe.redis.meta.server.keeper;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

/**
 * @author wenchao.meng
 *
 * Aug 5, 2016
 */
public interface DynamicStateChangeListener{
	
	void onMasterChanged(String clusterId, String shardId, RedisMeta newRedisMaster);
	
	void onKeeperStateChanged(String clusterId, String shardId, KeeperMeta keeperMeta);
}

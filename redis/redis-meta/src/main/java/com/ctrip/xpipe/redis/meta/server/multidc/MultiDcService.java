package com.ctrip.xpipe.redis.meta.server.multidc;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;

/**
 * @author wenchao.meng
 *
 * Dec 12, 2016
 */
public interface MultiDcService {
	
	KeeperMeta getActiveKeeper(String dcName, String clusterId, String shardId);

	RedisMeta getPeerMaster(String dcName, String clusterId, String shardId);

	RouteMeta getRouteMeta(String dcName, String clusterId);
}

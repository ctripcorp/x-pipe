package com.ctrip.xpipe.redis.meta.server.multidc;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

/**
 * @author wenchao.meng
 *
 * Dec 12, 2016
 */
public interface MultiDcService {
	
	KeeperMeta getActiveKeeper(String dcName, Long clusterDbId, Long shardDbId);

	RedisMeta getPeerMaster(String dcName, Long clusterDbId, Long shardDbId);

	String getSids(String dcName, Long clusterDbId, Long shardDbId);
}

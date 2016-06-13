package com.ctrip.xpipe.redis.keeper.meta;

import com.ctrip.xpipe.redis.keeper.entity.KeeperMeta;
import com.ctrip.xpipe.redis.keeper.entity.RedisMeta;

/**
 * @author marsqing
 *
 *         May 30, 2016 2:16:20 PM
 */
public interface MetaService {

	KeeperMeta getActiveKeeper(String clusterId, String shardId);

	RedisMeta getRedisMaster(String clusterId, String shardId);

	KeeperMeta getUpstreamKeeper(String clusterId, String shardId);
	
	String getActiveDc();

}

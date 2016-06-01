package com.ctrip.xpipe.redis.keeper.meta;

import com.ctrip.xpipe.redis.keeper.entity.Keeper;
import com.ctrip.xpipe.redis.keeper.entity.Redis;

/**
 * @author marsqing
 *
 * May 30, 2016 2:16:20 PM
 */
public interface MetaService {

	Keeper getActiveKeeper(String clusterId, String shardId);
	
	Redis  getRedisMaster(String clusterId, String shardId);
	
}

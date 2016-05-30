package com.ctrip.xpipe.redis.keeper.meta;

import com.ctrip.xpipe.redis.keeper.entity.Keeper;

/**
 * @author marsqing
 *
 * May 30, 2016 2:16:20 PM
 */
public interface MetaService {

	Keeper getActiveKeeper(String clusterId, String shardId);
	
}

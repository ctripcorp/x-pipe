package com.ctrip.xpipe.redis.keeper.meta;

/**
 * @author marsqing
 *
 *         May 30, 2016 2:16:20 PM
 */
public interface MetaService {
	
	ShardStatus getShardStatus(String clusterId, String shardId);

}

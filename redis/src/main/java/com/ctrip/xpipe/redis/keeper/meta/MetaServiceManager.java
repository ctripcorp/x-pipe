package com.ctrip.xpipe.redis.keeper.meta;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;

/**
 * @author wenchao.meng
 *
 * Jun 1, 2016
 */
public interface MetaServiceManager extends Observable{
	
	void add(RedisKeeperServer redisKeeperServer);

	ShardStatus getShardStatus(String clusterId, String shardId);
	
	void remove(RedisKeeperServer redisKeeperServer);

}

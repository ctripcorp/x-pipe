package com.ctrip.xpipe.redis.keeper.meta;

import com.ctrip.xpipe.api.observer.Observable;

/**
 * @author wenchao.meng
 *
 * Jun 1, 2016
 */
public interface MetaServiceManager extends Observable{
	
	void addShard(String clusterId, String shardId);

	ShardStatus getShardStatus(String clusterId, String shardId);
	
	void removeShard(String clusterId, String shardId);

}

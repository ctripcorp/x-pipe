package com.ctrip.xpipe.redis.console.service.meta;

import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
public interface RedisMetaService {

	RedisMeta getRedisMeta(ShardMeta shardMeta, RedisTbl redisInfo);
	
	KeeperMeta getKeeperMeta(ShardMeta shardMeta, RedisTbl redisInfo);

	void updateKeeperStatus(String dcId, String clusterId, String shardId, KeeperMeta newActiveKeeper) throws ResourceNotFoundException;
}

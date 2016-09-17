package com.ctrip.xpipe.redis.console.service.meta;

import java.util.Map;

import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.vo.DcMetaQueryVO;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
public interface RedisMetaService {
	
	String encodeRedisAddress(RedisTbl redisTbl);
	
	RedisMeta loadRedisMeta(ShardMeta shardMeta,RedisTbl redisTbl, DcMetaQueryVO dcMetaQueryVO); 
	
	KeeperMeta loadKeeperMeta(ShardMeta shardMeta, RedisTbl redisTbl, DcMetaQueryVO dcMetaQueryVO);
	
	RedisMeta getRedisMeta(ShardMeta shardMeta, RedisTbl redisInfo, Map<Long,RedisTbl> redises);
	
	KeeperMeta getKeeperMeta(ShardMeta shardMeta, RedisTbl redisInfo, Map<Long,RedisTbl> redises);

	void updateKeeperStatus(String dcId, String clusterId, String shardId, KeeperMeta newActiveKeeper);
}

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
	
	public String encodeRedisAddress(RedisTbl redisTbl);
	
	public RedisMeta loadRedisMeta(ShardMeta shardMeta,RedisTbl redisTbl, DcMetaQueryVO dcMetaQueryVO); 
	
	public KeeperMeta loadKeeperMeta(ShardMeta shardMeta, RedisTbl redisTbl, DcMetaQueryVO dcMetaQueryVO);
	
	public RedisMeta getRedisMeta(ShardMeta shardMeta, RedisTbl redisInfo, Map<Long,RedisTbl> redises);
	
	public KeeperMeta getKeeperMeta(ShardMeta shardMeta, RedisTbl redisInfo, Map<Long,RedisTbl> redises);
}

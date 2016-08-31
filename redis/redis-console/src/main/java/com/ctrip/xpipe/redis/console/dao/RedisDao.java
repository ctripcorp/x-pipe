package com.ctrip.xpipe.redis.console.dao;

import org.springframework.stereotype.Repository;

import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;

/**
 * @author shyin
 *
 * Aug 31, 2016
 */
@Repository
public class RedisDao  extends AbstractXpipeConsoleDAO{
	public RedisTbl bindRedis(DcClusterShardTbl dcClusterShard, RedisTbl redis) {
		return null;
	}
}

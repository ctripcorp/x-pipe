package com.ctrip.xpipe.redis.meta.server.dcchange;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;

/**
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
public interface SentinelManager {
	
	void addSentinel(String clusterId, String shardId, RedisMeta redisMaster, ExecutionLog executionLog);
	
	void removeSentinel(String clusterId, String shardId, ExecutionLog executionLog);

}

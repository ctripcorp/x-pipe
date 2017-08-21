package com.ctrip.xpipe.redis.meta.server.dcchange;

import com.ctrip.xpipe.endpoint.HostPort;

/**
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
public interface SentinelManager {
	
	void addSentinel(String clusterId, String shardId, HostPort redisMaster, ExecutionLog executionLog);
	
	void removeSentinel(String clusterId, String shardId, ExecutionLog executionLog);

}

package com.ctrip.xpipe.redis.meta.server.dcchange;

import com.ctrip.xpipe.endpoint.HostPort;

/**
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
public interface SentinelManager {
	
	void addSentinel(Long clusterDbId, Long shardDbId, HostPort redisMaster, ExecutionLog executionLog);
	
	void removeSentinel(Long clusterDbId, Long shardDbId, ExecutionLog executionLog);

}

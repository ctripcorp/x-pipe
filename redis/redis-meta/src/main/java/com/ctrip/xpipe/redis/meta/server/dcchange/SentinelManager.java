package com.ctrip.xpipe.redis.meta.server.dcchange;

/**
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
public interface SentinelManager {
	
	void addSentinel(String clusterId, String shardId);
	
	void removeSentinel(String clusterId, String shardId);

}

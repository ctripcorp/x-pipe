package com.ctrip.xpipe.redis.meta.server.impl.event;

import com.ctrip.xpipe.redis.meta.server.impl.MetaUpdated;

/**
 * @author wenchao.meng
 *
 * Jun 24, 2016
 */
public abstract class AbstractMetaUpdated implements MetaUpdated{
	
	private final String clusterId;
	private final String shardId;
	
	public AbstractMetaUpdated(String clusterId, String shardId){
		this.clusterId = clusterId;
		this.shardId = shardId;
	}
	
	public String getClusterId() {
		return clusterId;
	}

	public String getShardId() {
		return shardId;
	}
}

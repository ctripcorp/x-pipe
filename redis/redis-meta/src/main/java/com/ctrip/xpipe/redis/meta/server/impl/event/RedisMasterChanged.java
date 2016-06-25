package com.ctrip.xpipe.redis.meta.server.impl.event;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.impl.MetaUpdated;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public class RedisMasterChanged extends AbstractMetaUpdated implements MetaUpdated{
	
	private RedisMeta oldRedisMaster;
	private RedisMeta newRedisMaster;
	
	public RedisMasterChanged(String clusterId, String shardId, RedisMeta oldRedisMaster, RedisMeta newRedisMaster){
		super(clusterId, shardId);
		this.oldRedisMaster = oldRedisMaster;
		this.newRedisMaster = newRedisMaster;
	}
	
	public RedisMeta getOldRedisMaster() {
		return oldRedisMaster;
	}
	
	public RedisMeta getNewRedisMaster() {
		return newRedisMaster;
	}
	
}

package com.ctrip.xpipe.redis.meta.server.impl.event;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.impl.MetaUpdated;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public class ActiveKeeperChanged extends AbstractMetaUpdated implements MetaUpdated{
	
	private KeeperMeta oldKeeperMeta;
	private KeeperMeta newKeeperMeta;
	
	public ActiveKeeperChanged(String clusterId, String shardId, KeeperMeta oldKeeperMeta, KeeperMeta newKeeperMeta){
		super(clusterId, shardId);
		this.oldKeeperMeta = oldKeeperMeta;
		this.newKeeperMeta = newKeeperMeta;
	}
	
	public KeeperMeta getNewKeeperMeta() {
		return newKeeperMeta;
	}
	
	public KeeperMeta getOldKeeperMeta() {
		return oldKeeperMeta;
	}

}

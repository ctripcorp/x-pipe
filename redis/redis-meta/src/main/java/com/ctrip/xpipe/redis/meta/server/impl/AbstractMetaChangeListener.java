package com.ctrip.xpipe.redis.meta.server.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.impl.event.ActiveKeeperChanged;
import com.ctrip.xpipe.redis.meta.server.impl.event.RedisMasterChanged;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public abstract class AbstractMetaChangeListener implements MetaChangeListener{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	@Override
	public void update(Object args, Observable observable) {
		
		if(args instanceof ActiveKeeperChanged){
			
			ActiveKeeperChanged activeKeeperChanged = (ActiveKeeperChanged) args;
			activeKeeperChanged(activeKeeperChanged.getClusterId(), activeKeeperChanged.getShardId(), activeKeeperChanged.getOldKeeperMeta(), activeKeeperChanged.getNewKeeperMeta());
			return;
		}
		
		if(args instanceof RedisMasterChanged){
			
			RedisMasterChanged redisMasterChanged = (RedisMasterChanged) args;
			redisMasterChanged(redisMasterChanged.getClusterId(), redisMasterChanged.getShardId(), redisMasterChanged.getOldRedisMaster(), redisMasterChanged.getNewRedisMaster());
			return;
		}
		throw new IllegalStateException("unknown event:" + args.getClass() + "," + args);
	}

	protected abstract void redisMasterChanged(String clusterId, String shardId, RedisMeta oldRedisMaster, RedisMeta newRedisMaster);

	protected abstract void activeKeeperChanged(String clusterId, String shardId, KeeperMeta oldKeeperMeta, KeeperMeta newKeeperMeta);

}

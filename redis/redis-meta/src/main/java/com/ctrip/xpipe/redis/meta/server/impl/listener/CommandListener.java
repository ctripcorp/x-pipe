package com.ctrip.xpipe.redis.meta.server.impl.listener;

import org.springframework.stereotype.Component;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.impl.AbstractMetaChangeListener;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
@Component
public class CommandListener extends AbstractMetaChangeListener{

	@Override
	protected void redisMasterChanged(String clusterId, String shardId, RedisMeta oldRedisMaster,
			RedisMeta newRedisMaster) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void activeKeeperChanged(String clusterId, String shardId, KeeperMeta oldKeeperMeta,
			KeeperMeta newKeeperMeta) {
		// TODO Auto-generated method stub
		
	}

}

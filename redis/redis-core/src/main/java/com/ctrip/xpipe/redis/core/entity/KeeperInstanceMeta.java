package com.ctrip.xpipe.redis.core.entity;

import com.ctrip.xpipe.redis.core.store.ClusterId;
import com.ctrip.xpipe.redis.core.store.ShardId;

/**
 * @author wenchao.meng
 *
 * Aug 2, 2016
 */
public class KeeperInstanceMeta extends KeeperTransMeta{


	public KeeperInstanceMeta(){
		
	}

	public KeeperInstanceMeta(ClusterId clusterId, ShardId shardId, KeeperMeta keeperMeta) {
		super(clusterId.id(), shardId.id(), keeperMeta);
	}

}

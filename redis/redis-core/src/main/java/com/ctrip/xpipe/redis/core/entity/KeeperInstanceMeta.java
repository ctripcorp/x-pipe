package com.ctrip.xpipe.redis.core.entity;

import com.ctrip.xpipe.redis.core.store.ReplId;

/**
 * @author wenchao.meng
 *
 * Aug 2, 2016
 */
public class KeeperInstanceMeta extends KeeperTransMeta{


	public KeeperInstanceMeta(){
		
	}

	public KeeperInstanceMeta(ReplId replId, KeeperMeta keeperMeta) {
		super(replId.id(), keeperMeta);
	}

}

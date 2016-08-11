package com.ctrip.xpipe.redis.core.entity;

/**
 * @author wenchao.meng
 *
 * Aug 2, 2016
 */
public class KeeperInstanceMeta extends KeeperTransMeta{


	public KeeperInstanceMeta(){
		
	}

	public KeeperInstanceMeta(String clusterId, String shardId, KeeperMeta keeperMeta) {
		super(clusterId, shardId, keeperMeta);
	}

}

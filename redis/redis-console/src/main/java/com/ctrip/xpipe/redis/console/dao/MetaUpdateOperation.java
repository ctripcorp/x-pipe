package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public interface MetaUpdateOperation {
	
	/**
	 * @param dc
	 * @param clusterId
	 * @param shardId
	 * @param activeKeeper
	 * @return true if active changed
	 */
	boolean updateKeeperActive(String dc, String clusterId, String shardId, KeeperMeta activeKeeper);
}	


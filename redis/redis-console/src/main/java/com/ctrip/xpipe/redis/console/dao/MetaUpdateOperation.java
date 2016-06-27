package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

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
	 * @throws DaoException 
	 */
	boolean updateKeeperActive(String dc, String clusterId, String shardId, KeeperMeta activeKeeper) throws DaoException;
	
	boolean updateRedisMaster(String dc, String clusterId, String shardId, RedisMeta redisMaster) throws DaoException;
}	


package com.ctrip.xpipe.redis.core.dao;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
public interface MetaUpdateOperation {

	boolean updateKeeperActive(String dc, String clusterId, String shardId, KeeperMeta activeKeeper) throws DaoException;
	
	boolean updateRedisMaster(String dc, String clusterId, String shardId, RedisMeta redisMaster) throws DaoException;

}

package com.ctrip.xpipe.redis.meta.server;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

/**
 * @author wenchao.meng
 *
 * Aug 7, 2016
 */
public interface MetaServerEventsHandler {

	/**
	 * 1. make sure keeper is at proper state
	 * 2. notify console
	 * @param clusterId
	 * @param shardId
	 * @param activeKeeper
	 * @throws Exception
	 */
	void keeperActiveElected(String clusterId, String shardId, KeeperMeta activeKeeper) throws Exception;
	
	/**
	 * 1. update dynamic state in memory
	 * 2. update console
	 * @param clusterId
	 * @param shardId
	 * @param redisMaster
	 * @throws Exception
	 */
	void redisMasterChanged(String clusterId, String shardId, RedisMeta redisMaster) throws Exception;

}

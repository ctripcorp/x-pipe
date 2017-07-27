package com.ctrip.xpipe.redis.meta.server;

import org.unidal.tuple.Pair;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;

/**
 * @author wenchao.meng
 *
 * Aug 7, 2016
 */
public interface MetaServerStateChangeHandler {

	/**
	 * 1. make sure keeper is at proper state
	 * 2. notify console
	 * @param clusterId
	 * @param shardId
	 * @param activeKeeper if activeKeeper == null, means that no keeper is active 
	 * @throws Exception
	 */
	void keeperActiveElected(String clusterId, String shardId, KeeperMeta activeKeeper);

	void keeperMasterChanged(String clusterId, String shardId, Pair<String, Integer> newMaster);
}

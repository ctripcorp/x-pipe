package com.ctrip.xpipe.redis.meta.server;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.tuple.Pair;

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
	default void keeperActiveElected(String clusterId, String shardId, KeeperMeta activeKeeper) {}

	default void keeperMasterChanged(String clusterId, String shardId, Pair<String, Integer> newMaster) {}

	default void currentMasterChanged(String clusterId, String shardId) {}

	default void peerMasterChanged(String dcId, String clusterId, String shardId) {}

}

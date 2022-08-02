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
	 * @param clusterDbId
	 * @param shardDbId
	 * @param activeKeeper if activeKeeper == null, means that no keeper is active 
	 * @throws Exception
	 */
	default void keeperActiveElected(Long clusterDbId, Long shardDbId, KeeperMeta activeKeeper) {}

	default void keeperMasterChanged(Long clusterDbId, Long shardDbId, Pair<String, Integer> newMaster) {}

	default void currentMasterChanged(Long clusterDbId, Long shardDbId) {}

	default void peerMasterChanged(Long clusterDbId, Long shardDbId) {}

}

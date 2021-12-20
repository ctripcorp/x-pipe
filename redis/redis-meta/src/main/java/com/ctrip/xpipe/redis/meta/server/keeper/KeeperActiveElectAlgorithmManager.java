package com.ctrip.xpipe.redis.meta.server.keeper;


/**
 * @author wenchao.meng
 *
 * Aug 6, 2016
 */
public interface KeeperActiveElectAlgorithmManager {

	KeeperActiveElectAlgorithm get(Long clusterDbId, Long shardDbId);
	
}

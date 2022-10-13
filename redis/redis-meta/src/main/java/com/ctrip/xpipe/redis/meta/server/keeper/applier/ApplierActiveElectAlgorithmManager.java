package com.ctrip.xpipe.redis.meta.server.keeper.applier;


/**
 * @author ayq
 * <p>
 * 2022/4/11 22:08
 */
public interface ApplierActiveElectAlgorithmManager {

    ApplierActiveElectAlgorithm get(Long clusterDbId, Long shardDbId);
}

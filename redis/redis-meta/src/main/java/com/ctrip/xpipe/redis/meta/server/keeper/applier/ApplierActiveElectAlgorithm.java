package com.ctrip.xpipe.redis.meta.server.keeper.applier;

import com.ctrip.xpipe.redis.core.entity.ApplierMeta;

import java.util.List;

/**
 * @author ayq
 * <p>
 * 2022/4/11 22:09
 */
public interface ApplierActiveElectAlgorithm {

    ApplierMeta select(Long clusterDbId, Long shardDbId, List<ApplierMeta> toBeSelected);
}

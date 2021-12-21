package com.ctrip.xpipe.redis.meta.server.keeper;


import com.ctrip.xpipe.redis.core.entity.KeeperMeta;

import java.util.List;

/**
 * @author wenchao.meng
 *
 * Aug 6, 2016
 */
public interface KeeperActiveElectAlgorithm {

	KeeperMeta select(Long clusterDbId, Long shardDbId, List<KeeperMeta> toBeSelected);
}

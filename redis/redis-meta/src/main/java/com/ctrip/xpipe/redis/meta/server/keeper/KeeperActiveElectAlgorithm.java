package com.ctrip.xpipe.redis.meta.server.keeper;


import java.util.List;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;

/**
 * @author wenchao.meng
 *
 * Aug 6, 2016
 */
public interface KeeperActiveElectAlgorithm {

	KeeperMeta select(String clusterId, String shardId, List<KeeperMeta> toBeSelected);
}

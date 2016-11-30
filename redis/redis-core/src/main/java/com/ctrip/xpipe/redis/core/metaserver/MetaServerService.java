package com.ctrip.xpipe.redis.core.metaserver;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;

/**
 * @author wenchao.meng
 *
 * Aug 2, 2016
 */
public interface MetaServerService {

	public static final String HTTP_HEADER_FOWRARD = "forward";

	KeeperMeta getActiveKeeper(String clusterId, String shardId);

	
}

package com.ctrip.xpipe.redis.meta.server.multidc;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.ProxyRedisMeta;

/**
 * @author wenchao.meng
 *
 * Dec 12, 2016
 */
public interface MultiDcService {
	
	KeeperMeta getActiveKeeper(String dcName, String clusterId, String shardId);

	ProxyRedisMeta getPeerMaster(String dcName, String clusterId, String shardId);

}

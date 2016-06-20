package com.ctrip.xpipe.redis.meta.server;

import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;




/**
 * @author marsqing
 *
 *         May 25, 2016 2:37:05 PM
 */
public interface MetaServer extends TopElement {

	KeeperMeta getActiveKeeper(String clusterId, String shardId);

	RedisMeta getRedisMaster(String clusterId, String shardId);

	KeeperMeta getUpstreamKeeper(String clusterId, String shardId);

	void watchCluster(ClusterMeta cluster) throws Exception;
}

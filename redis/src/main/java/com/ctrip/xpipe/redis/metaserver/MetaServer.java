package com.ctrip.xpipe.redis.metaserver;


import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.redis.keeper.entity.ClusterMeta;
import com.ctrip.xpipe.redis.keeper.entity.KeeperMeta;
import com.ctrip.xpipe.redis.keeper.entity.RedisMeta;

/**
 * @author marsqing
 *
 *         May 25, 2016 2:37:05 PM
 */
public interface MetaServer extends Lifecycle {

	KeeperMeta getActiveKeeper(String clusterId, String shardId);

	RedisMeta getRedisMaster(String clusterId, String shardId);

	/**
	 * @param cluster
	 * @throws Exception 
	 */
	void watchCluster(ClusterMeta cluster) throws Exception;
}

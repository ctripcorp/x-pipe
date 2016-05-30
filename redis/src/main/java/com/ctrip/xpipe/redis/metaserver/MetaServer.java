package com.ctrip.xpipe.redis.metaserver;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.redis.keeper.entity.Cluster;
import com.ctrip.xpipe.redis.keeper.entity.Keeper;
import com.ctrip.xpipe.redis.keeper.entity.Redis;

/**
 * @author marsqing
 *
 *         May 25, 2016 2:37:05 PM
 */
public interface MetaServer extends Lifecycle {

	Keeper getActiveKeeper(String clusterId, String shardId);

	Redis getRedisMaster(String clusterId, String shardId);

	/**
	 * @param cluster
	 * @throws Exception 
	 */
	void watchCluster(Cluster cluster) throws Exception;
}

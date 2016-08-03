package com.ctrip.xpipe.redis.meta.server;



import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.ShardStatus;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServer;
import com.ctrip.xpipe.redis.meta.server.rest.AllMetaServerService;

/**
 * @author marsqing
 *
 *         May 25, 2016 2:37:05 PM
 */
public interface MetaServer extends ClusterServer, AllMetaServerService, TopElement{

	KeeperMeta getActiveKeeper(String clusterId, String shardId);

	RedisMeta getRedisMaster(String clusterId, String shardId);

	KeeperMeta getUpstreamKeeper(String clusterId, String shardId) throws Exception;

	ShardStatus getShardStatus(String clusterId, String shardId) throws Exception;

	void updateActiveKeeper(String clusterId, String shardId, KeeperMeta keeper) throws Exception;
	
	void updateUpstream(String clusterId, String shardId, String upstream) throws Exception;
	
	void promoteRedisMaster(String clusterId, String shardId, String promoteIp, int promotePort) throws Exception;

}

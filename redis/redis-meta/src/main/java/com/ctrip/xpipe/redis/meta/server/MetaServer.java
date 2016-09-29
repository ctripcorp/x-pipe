package com.ctrip.xpipe.redis.meta.server;




import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.ShardStatus;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServer;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfo;

/**
 * @author marsqing
 *
 *         May 25, 2016 2:37:05 PM
 */
public interface MetaServer extends ClusterServer, TopElement{

	KeeperMeta getActiveKeeper(String clusterId, String shardId);

	RedisMeta getRedisMaster(String clusterId, String shardId);

	KeeperMeta getUpstreamKeeper(String clusterId, String shardId) throws Exception;

	void updateUpstream(String clusterId, String shardId, String ip, int port, ForwardInfo forwardInfo) throws Exception;

	ShardStatus getShardStatus(String clusterId, String shardId, ForwardInfo forwardInfo) throws Exception;

	void ping(String clusterId, String shardId, KeeperInstanceMeta keeperInstanceMeta, ForwardInfo forwardInfo);
	
	void clusterAdded(ClusterMeta clusterMeta, ForwardInfo forwardInfo);

	void clusterModified(ClusterMeta clusterMeta, ForwardInfo forwardInfo);

	void clusterDeleted(String clusterId, ForwardInfo forwardInfo);

	String getCurrentMeta();

}

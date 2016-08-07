package com.ctrip.xpipe.redis.meta.server.keeper;

import java.util.List;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
public interface KeeperElectorManager {
	
	/**
	 * @return allkeerps registed to zookeeper
	 */
	List<KeeperMeta>  getAllAliveKeepers(String clusterId, String shardId);
	
	KeeperMeta  getActive(String clusterId, String shardId);

	void watchCluster(ClusterMeta clusterMeta) throws Exception;

	void unwatchCluster(ClusterMeta clusterMeta) throws Exception;

}

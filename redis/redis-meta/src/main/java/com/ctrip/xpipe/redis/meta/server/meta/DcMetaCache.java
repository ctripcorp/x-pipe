package com.ctrip.xpipe.redis.meta.server.meta;

import java.util.List;
import java.util.Set;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

/**
 * @author wenchao.meng
 *
 * Aug 7, 2016
 */
public interface DcMetaCache extends Observable{

	Set<String> getClusters();

	String getCurrentDc();

	ClusterMeta getClusterMeta(String clusterId);

	KeeperContainerMeta getKeeperContainer(KeeperMeta keeperMeta);

	void clusterAdded(ClusterMeta clusterMeta);

	void clusterModified(ClusterMeta clusterMeta);

	void clusterDeleted(String clusterId);

	boolean isCurrentDcPrimary(String clusterId, String shardId);

	List<KeeperMeta> getShardKeepers(String clusterId, String shardId);
	
	List<RedisMeta>  getShardRedises(String clusterId, String shardId);
}

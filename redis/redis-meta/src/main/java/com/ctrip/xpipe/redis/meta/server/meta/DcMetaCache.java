package com.ctrip.xpipe.redis.meta.server.meta;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.core.entity.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author wenchao.meng
 *
 *         Aug 7, 2016
 */
public interface DcMetaCache extends Observable {

	Set<String> getClusters();

	String getCurrentDc();

	ClusterMeta getClusterMeta(String clusterId);

	ClusterType getClusterType(String clusterId);

	RouteMeta randomRoute(String clusterId);

	List<RouteMeta> getAllRoutes();

	KeeperContainerMeta getKeeperContainer(KeeperMeta keeperMeta);

	boolean isCurrentDcPrimary(String clusterId, String shardId);

	boolean isCurrentDcPrimary(String clusterId);

	Set<String> getBakupDcs(String clusterId, String shardId);

	Set<String> getRelatedDcs(String clusterId, String shardId);

	String getPrimaryDc(String clusterId, String shardId);

	List<KeeperMeta> getShardKeepers(String clusterId, String shardId);

	List<RedisMeta> getShardRedises(String clusterId, String shardId);

	SentinelMeta getSentinel(String clusterId, String shardId);

	String getSentinelMonitorName(String clusterId, String shardId);
	
	
	void clusterAdded(ClusterMeta clusterMeta);

	void clusterModified(ClusterMeta clusterMeta);

	void clusterDeleted(String clusterId);
	
	void primaryDcChanged(String clusterId, String shardId, String newPrimaryDc);
}

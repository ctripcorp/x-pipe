package com.ctrip.xpipe.redis.meta.server.meta;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.tuple.Pair;

import java.util.List;
import java.util.Set;

/**
 * @author wenchao.meng
 *
 *         Aug 7, 2016
 */
public interface DcMetaCache extends Observable {

	String clusterDbId2Name(Long clusterDbId);

	Pair<String, String> clusterShardDbId2Name(Long clusterDbId, Long shardDbId);

	Long clusterId2DbId(String clusterId);

	Pair<Long, Long> clusterShardId2DbId(String clusterId, String shardId);

	Set<ClusterMeta> getClusters();

	String getCurrentDc();

	ClusterMeta getClusterMeta(Long clusterDbId);

	ClusterType getClusterType(Long clusterDbId);

	RouteMeta randomRoute(Long clusterDbId);

	List<RouteMeta> getAllRoutes();

	KeeperContainerMeta getKeeperContainer(KeeperMeta keeperMeta);

	boolean isCurrentDcPrimary(Long clusterDbId, Long shardDbId);

	boolean isCurrentDcPrimary(Long clusterDbId);

	Set<String> getBakupDcs(Long clusterDbId, Long shardDbId);

	Set<String> getRelatedDcs(Long clusterDbId, Long shardDbId);

	String getPrimaryDc(Long clusterDbId, Long shardDbId);

	List<KeeperMeta> getShardKeepers(Long clusterDbId, Long shardDbId);

	List<RedisMeta> getShardRedises(Long clusterDbId, Long shardDbId);

	SentinelMeta getSentinel(Long clusterDbId, Long shardDbId);

	String getSentinelMonitorName(Long clusterDbId, Long shardDbId);
	
	void clusterAdded(ClusterMeta clusterMeta);

	void clusterModified(ClusterMeta clusterMeta);

	void clusterDeleted(Long clusterDbId);
	
	void primaryDcChanged(Long clusterDbId, Long shardDbId, String newPrimaryDc);

}

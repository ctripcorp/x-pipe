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

	List<RouteMeta> getAllMetaRoutes();

	RouteMeta chooseRoute(long clusterDbId, String dstDcId);

	KeeperContainerMeta getKeeperContainer(KeeperMeta keeperMeta);

	/** All keeper containers known to the current dc's meta — for the orphan-store GC round's
	 * logicalBu/tfsFsId grouping, which needs the full container population, not just the one a
	 * given keeper happens to sit on. */
	List<KeeperContainerMeta> getKeeperContainers();

	ApplierContainerMeta getApplierContainer(ApplierMeta applierMeta);

	boolean isCurrentDcPrimary(Long clusterDbId, Long shardDbId);

	boolean isCurrentDcPrimary(Long clusterDbId);

	boolean isCurrentDcBackUp(Long clusterDbId, Long shardDbId);

	boolean isCurrentDcBackUp(Long clusterDbId);

	boolean isCurrentShardParentCluster(Long clusterDbId, Long shardDbId);

	Set<String> getBakupDcs(Long clusterDbId, Long shardDbId);

	Set<String> getDownstreamDcs(String dc, Long clusterDbId, Long shardDbId);

	String getUpstreamDc(String dc, Long clusterDbId, Long shardDbId);

	String getSrcDc(String dc, Long clusterDbId, Long shardDbId);

	Set<String> getRelatedDcs(Long clusterDbId, Long shardDbId);

	String getPrimaryDc(Long clusterDbId, Long shardDbId);

	List<KeeperMeta> getShardKeepers(Long clusterDbId, Long shardDbId);

	List<ApplierMeta> getShardAppliers(Long clusterDbId, Long shardDbId);

	List<RedisMeta> getClusterRedises(Long clusterDbId);

	List<RedisMeta> getShardRedises(Long clusterDbId, Long shardDbId);

	SentinelMeta getSentinel(Long clusterDbId, Long shardDbId);

	String getSentinelMonitorName(Long clusterDbId, Long shardDbId);

	void clusterAdded(ClusterMeta clusterMeta);

	void clusterModified(ClusterMeta clusterMeta);

	void clusterDeleted(Long clusterDbId);
	
	void primaryDcChanged(Long clusterDbId, Long shardDbId, String newPrimaryDc);

	/**
	 * Wall-clock time of the last {@code loadMetaFromConsole()} call that returned without throwing,
	 * regardless of whether it actually changed anything — distinct from "last time the meta actually
	 * changed", which can go arbitrarily long on a healthy, stable dc. The orphan-store GC round uses
	 * this (not meta-change time) to detect "console/qconfig has been failing to refresh for a while",
	 * which is the actual danger signal for a destructive decision.
	 */
	long getLastSuccessfulLoadMilli();

}

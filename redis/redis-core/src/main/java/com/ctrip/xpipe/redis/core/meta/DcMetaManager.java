package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.tuple.Pair;

import java.util.List;
import java.util.Set;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
public interface DcMetaManager{

	/**
	 * if no route found return null
	 * @param clusterId
	 * @return
	 */
//	RouteMeta randomRoute(String clusterId);

	List<RouteMeta> getAllMetaRoutes();

	/**
	 * find all clusters in currentDc whose active dc is clusterActiveDc
	 * @param clusterActiveDc
	 * @return
	 */
	List<ClusterMeta> getSpecificActiveDcClusters(String clusterActiveDc);


	Set<ClusterMeta> getClusters();

	boolean hasCluster(String clusterId);
	
	boolean hasShard(String clusterId, String shardId);
	
	ClusterMeta getClusterMeta(String clusterId);

	ClusterType getClusterType(String clusterId);
	
	String getActiveDc(String clusterId, String shardId);
	
	SentinelMeta getSentinel(String clusterId, String shardId);
	
	String getSentinelMonitorName(String clusterId, String shardId);

	ShardMeta getShardMeta(String clusterId, String shardId);

	List<KeeperMeta> getKeepers(String clusterId, String shardId);

	List<RedisMeta> getRedises(String clusterId, String shardId);

	KeeperMeta getKeeperActive(String clusterId, String shardId);
	
	List<KeeperMeta> getKeeperBackup(String clusterId, String shardId);
	
	/**
	 * @param clusterId
	 * @param shardId
	 * @return dc and redismeta info
	 */
	RedisMeta getRedisMaster(String clusterId, String shardId);
	
	List<MetaServerMeta> getMetaServers();
	
	ZkServerMeta  getZkServerMeta();

	KeeperContainerMeta getKeeperContainer(KeeperMeta keeperMeta);

	DcMeta getDcMeta();
	
	List<KeeperMeta> getAllSurviveKeepers(String clusterId, String shardId);

	void update(ClusterMeta clusterMeta);

	ClusterMeta removeCluster(String clusterId);

	boolean updateKeeperActive(String clusterId, String shardId, KeeperMeta activeKeeper);
	
	boolean noneKeeperActive(String clusterId, String shardId);
	
	boolean updateRedisMaster(String clusterId, String shardId, RedisMeta redisMaster);

	void setSurviveKeepers(String clusterId, String shardId, List<KeeperMeta> surviceKeepers);

	Set<String> getBackupDcs(String clusterId, String shardId);

	Set<String> getRelatedDcs(String clusterId, String shardId);

	void primaryDcChanged(String clusterId, String shardId, String newPrimaryDc);

	/* method use clusterDbId, shardDbId */

	String clusterDbId2Name(Long clusterDbId);

	Pair<String, String> clusterShardDbId2Name(Long clusterDbId, Long shardDbId);

	Long clusterId2DbId(String clusterId);

	Pair<Long, Long> clusterShardId2DbId(String clusterId, String shardId);

//	RouteMeta randomRoute(Long clusterDbId);

	boolean hasCluster(Long clusterDbId);

	boolean hasShard(Long clusterDbId, Long shardDbId);

	ClusterMeta getClusterMeta(Long clusterDbId);

	ClusterType getClusterType(Long clusterDbId);

	String getActiveDc(Long clusterDbId, Long shardDbId);

	SentinelMeta getSentinel(Long clusterDbId, Long shardDbId);

	String getSentinelMonitorName(Long clusterDbId, Long shardDbId);

	ShardMeta getShardMeta(Long clusterDbId, Long shardDbId);

	List<KeeperMeta> getKeepers(Long clusterDbId, Long shardDbId);

	List<RedisMeta> getRedises(Long clusterDbId, Long shardDbId);

	KeeperMeta getKeeperActive(Long clusterDbId, Long shardDbId);

	List<KeeperMeta> getKeeperBackup(Long clusterDbId, Long shardDbId);

	/**
	 * @param clusterDbId
	 * @param shardDbId
	 * @return dc and redismeta info
	 */
	RedisMeta getRedisMaster(Long clusterDbId, Long shardDbId);

	List<KeeperMeta> getAllSurviveKeepers(Long clusterDbId, Long shardDbId);

	ClusterMeta removeCluster(Long clusterDbId);

	boolean updateKeeperActive(Long clusterDbId, Long shardDbId, KeeperMeta activeKeeper);

	boolean noneKeeperActive(Long clusterDbId, Long shardDbId);

	boolean updateRedisMaster(Long clusterDbId, Long shardDbId, RedisMeta redisMaster);

	void setSurviveKeepers(Long clusterDbId, Long shardDbId, List<KeeperMeta> surviceKeepers);

	Set<String> getBackupDcs(Long clusterDbId, Long shardDbId);

	Set<String> getRelatedDcs(Long clusterDbId, Long shardDbId);

	void primaryDcChanged(Long clusterDbId, Long shardDbId, String newPrimaryDc);

}

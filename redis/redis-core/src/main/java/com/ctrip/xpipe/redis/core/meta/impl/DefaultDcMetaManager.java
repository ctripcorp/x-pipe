package com.ctrip.xpipe.redis.core.meta.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.DcMetaManager;
import com.ctrip.xpipe.redis.core.meta.MetaClone;
import com.ctrip.xpipe.redis.core.meta.MetaException;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategy;
import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
public final class DefaultDcMetaManager implements DcMetaManager{
	
	protected XpipeMetaManager metaManager;
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	protected String currentDc;

	protected Map<Long, ClusterSummary> clusterDbIdMap;

	private DefaultDcMetaManager(String currentDc, XpipeMetaManager xpipeMetaManager){
		this.metaManager = xpipeMetaManager;
		this.currentDc = currentDc;
		this.buildClusterDbIdMap();
	}

	public static DcMetaManager buildFromFile(String dcId, String fileName){
		
		return new DefaultDcMetaManager(dcId, DefaultXpipeMetaManager.buildFromFile(fileName));
	}

	public static DcMetaManager buildForDc(String dcId){
		
		XpipeMeta xpipeMeta = new XpipeMeta();
		DcMeta dcMeta = new DcMeta();
		dcMeta.setId(dcId);
		xpipeMeta.addDc(dcMeta);
		return new DefaultDcMetaManager(dcId, DefaultXpipeMetaManager.buildFromMeta(xpipeMeta));
	}

	public static DcMetaManager buildFromDcMeta(DcMeta dcMeta){
		
		XpipeMeta xpipeMeta = new XpipeMeta();
		xpipeMeta.addDc(dcMeta);
		return new DefaultDcMetaManager(dcMeta.getId(), DefaultXpipeMetaManager.buildFromMeta(xpipeMeta));
	}

	@Override
	public ClusterMeta getClusterMeta(String clusterId) {
		
		return metaManager.getClusterMeta(currentDc, clusterId);
	}

	@Override
	public ClusterType getClusterType(String clusterId) {
		return metaManager.getClusterType(clusterId);
	}

	@Override
	public ShardMeta getShardMeta(String clusterId, String shardId) {
		return metaManager.getShardMeta(currentDc, clusterId, shardId);
	}

	@Override
	public List<KeeperMeta> getKeepers(String clusterId, String shardId) {
		return metaManager.getKeepers(currentDc, clusterId, shardId);
	}

	@Override
	public List<RedisMeta> getRedises(String clusterId, String shardId) {
		return metaManager.getRedises(currentDc, clusterId, shardId);
	}

	@Override
	public KeeperMeta getKeeperActive(String clusterId, String shardId) {
		return metaManager.getKeeperActive(currentDc, clusterId, shardId);
	}

	@Override
	public List<KeeperMeta> getKeeperBackup(String clusterId, String shardId) {
		return metaManager.getKeeperBackup(currentDc, clusterId, shardId);
	}

	@Override
	public RedisMeta getRedisMaster(String clusterId, String shardId) {
		
		List<RedisMeta> allRedises = getRedises(clusterId, shardId);
		if(allRedises == null){
			return null;
		}
		
		for(RedisMeta redisMeta : allRedises){
			if(redisMeta.isMaster()){
				return redisMeta;
			}
		}
		return null;
	}

	@Override
	public List<MetaServerMeta> getMetaServers() {
		return metaManager.getMetaServers(currentDc);
	}

	@Override
	public ZkServerMeta getZkServerMeta() {
		return metaManager.getZkServerMeta(currentDc);
	}

	@Override
	public Set<ClusterMeta> getClusters() {
		return metaManager.getDcClusters(currentDc);
	}


	@Override
	public List<RouteMeta> getAllMetaRoutes() {
		return metaManager.metaRoutes(currentDc);
	}

	@Override
	public Map<String, RouteMeta> chooseRoutes(List<String> dstDcs, int orgId, RouteChooseStrategy strategy,
											   Map<String, List<RouteMeta>> clusterPrioritizedRoutes) {
		return metaManager.chooseMetaRoutes(currentDc, dstDcs, orgId, clusterPrioritizedRoutes, strategy);
	}

	@Override
	public List<ClusterMeta> getSpecificActiveDcClusters(String clusterActiveDc) {

		return metaManager.getSpecificActiveDcClusters(currentDc, clusterActiveDc);
	}

	@Override
	public KeeperContainerMeta getKeeperContainer(KeeperMeta keeperMeta) {
		return metaManager.getKeeperContainer(currentDc, keeperMeta);
	}
	
	protected void update(DcMeta dcMeta) {
		metaManager.update(dcMeta);
	}
	
	@Override
	public void update(ClusterMeta clusterMeta){
		metaManager.update(currentDc, clusterMeta);
		buildClusterDbIdMap();
	}


	@Override
	public ClusterMeta removeCluster(String clusterId) {
		return metaManager.removeCluster(currentDc, clusterId);
	}


	@Override
	public boolean updateKeeperActive(String clusterId, String shardId, KeeperMeta activeKeeper) throws MetaException {
		return metaManager.updateKeeperActive(currentDc, clusterId, shardId, activeKeeper);
	}

	@Override
	public boolean noneKeeperActive(String clusterId, String shardId) {
		return metaManager.noneKeeperActive(currentDc, clusterId, shardId) ;
	}

	@Override
	public boolean updateRedisMaster(String clusterId, String shardId, RedisMeta redisMaster) {
		return metaManager.updateRedisMaster(currentDc, clusterId, shardId, redisMaster);
	}

	@Override
	public DcMeta getDcMeta() {
		return MetaClone.clone(metaManager.getDcMeta(currentDc));
	}


	@Override
	public List<KeeperMeta> getAllSurviveKeepers(String clusterId, String shardId) {
		return metaManager.getAllSurviveKeepers(currentDc, clusterId, shardId);
	}

	@Override
	public void setSurviveKeepers(String clusterId, String shardId, List<KeeperMeta> surviceKeepers) {
		metaManager.setSurviveKeepers(currentDc, clusterId, shardId, surviceKeepers);
	}

	@Override
	public String toString() {
		
		DcMeta dcMeta = metaManager.getDcMeta(currentDc);
		return String.format("dc:%s, meta:%s", currentDc, dcMeta);
	}


	@Override
	public boolean hasCluster(String clusterId) {
		return metaManager.hasCluster(currentDc, clusterId);
	}

	@Override
	public boolean hasShard(String clusterId, String shardId) {
		return metaManager.hasShard(currentDc, clusterId, shardId);
	}

	@Override
	public String getActiveDc(String clusterId, String shardId) {
		return metaManager.getActiveDc(clusterId, shardId);
	}

	@Override
	public Set<String> getBackupDcs(String clusterId, String shardId) {
		
		return metaManager.getBackupDcs(clusterId, shardId);
	}

	@Override
	public Set<String> getRelatedDcs(String clusterId, String shardId) {
		return metaManager.getRelatedDcs(clusterId, shardId);
	}


	@Override
	public SentinelMeta getSentinel(String clusterId, String shardId) {
		return metaManager.getSentinel(currentDc, clusterId, shardId);
	}


	@Override
	public String getSentinelMonitorName(String clusterId, String shardId) {
		
		return metaManager.getShardMeta(currentDc, clusterId, shardId).getSentinelMonitorName();
	}


	@Override
	public void primaryDcChanged(String clusterId, String shardId, String newPrimaryDc) {
		metaManager.primaryDcChanged(currentDc, clusterId, shardId, newPrimaryDc);
	}

	private static class ClusterSummary {
		String name;
		Map<Long, String> shards;

		public ClusterSummary(String name) {
			this.name = name;
			this.shards = new HashMap<>();
		}
	}

	private void buildClusterDbIdMap() {
		Map<Long, ClusterSummary> localClusterDbIdMap = new HashMap<>();
		DcMeta dcMeta = this.metaManager.getDcMeta(currentDc);
		for (ClusterMeta clusterMeta: dcMeta.getClusters().values()) {
			ClusterSummary clusterSummary = new ClusterSummary(clusterMeta.getId());
			for (ShardMeta shardMeta: clusterMeta.getShards().values()) {
				clusterSummary.shards.put(shardMeta.getDbId(), shardMeta.getId());
			}

			localClusterDbIdMap.put(clusterMeta.getDbId(), clusterSummary);
		}

		this.clusterDbIdMap = localClusterDbIdMap;
	}

	@Override
	public String clusterDbId2Name(Long clusterDbId) {
		ClusterSummary clusterSummary = clusterDbIdMap.get(clusterDbId);
		if (null == clusterSummary) {
			throw new IllegalArgumentException("unknown clusterDbId " + clusterDbId);
		}
		return clusterSummary.name;
	}

	@Override
	public Pair<String, String> clusterShardDbId2Name(Long clusterDbId, Long shardDbId) {
		ClusterSummary clusterSummary = clusterDbIdMap.get(clusterDbId);
		if (null == clusterSummary || !clusterSummary.shards.containsKey(shardDbId)) {
			throw new IllegalArgumentException(String.format("unknown clusterDbId shardDbId %d %d", clusterDbId, shardDbId));
		}
		return Pair.of(clusterSummary.name, clusterSummary.shards.get(shardDbId));
	}

	@Override
	public Long clusterId2DbId(String clusterId) {
		ClusterMeta clusterMeta = getClusterMeta(clusterId);
		if (null == clusterMeta) {
			throw new IllegalArgumentException("unknown clusterId " + clusterId);
		}
		return getClusterMeta(clusterId).getDbId();
	}

	@Override
	public Pair<Long, Long> clusterShardId2DbId(String clusterId, String shardId) {
		ClusterMeta clusterMeta = getClusterMeta(clusterId);
		if (null == clusterMeta || !clusterMeta.getShards().containsKey(shardId)) {
			throw new IllegalArgumentException(String.format("unknown clusterId shardId %s %s", clusterId, shardId));
		}
		ShardMeta shardMeta = clusterMeta.getShards().get(shardId);
		return Pair.of(clusterMeta.getDbId(), shardMeta.getDbId());
	}

	@Override
	public boolean hasCluster(Long clusterDbId) {
		ClusterSummary clusterSummary = clusterDbIdMap.get(clusterDbId);
		return null!= clusterSummary && hasCluster(clusterSummary.name);
	}

	@Override
	public boolean hasShard(Long clusterDbId, Long shardDbId) {
		ClusterSummary clusterSummary = clusterDbIdMap.get(clusterDbId);
		return null != clusterSummary && clusterSummary.shards.containsKey(shardDbId)
				&& hasShard(clusterSummary.name, clusterSummary.shards.get(shardDbId));
	}

	@Override
	public ClusterMeta getClusterMeta(Long clusterDbId) {
		ClusterSummary clusterSummary = clusterDbIdMap.get(clusterDbId);
		if (null == clusterSummary) return null;
		return getClusterMeta(clusterSummary.name);
	}

	@Override
	public ClusterType getClusterType(Long clusterDbId) {
		return getClusterType(clusterDbId2Name(clusterDbId));
	}

	@Override
	public String getActiveDc(Long clusterDbId, Long shardDbId) {
		return getActiveDc(clusterDbId2Name(clusterDbId), null);
	}

	@Override
	public SentinelMeta getSentinel(Long clusterDbId, Long shardDbId) {
		Pair<String, String> clusterShard = clusterShardDbId2Name(clusterDbId, shardDbId);
		return getSentinel(clusterShard.getKey(), clusterShard.getValue());
	}

	@Override
	public String getSentinelMonitorName(Long clusterDbId, Long shardDbId) {
		Pair<String, String> clusterShard = clusterShardDbId2Name(clusterDbId, shardDbId);
		return getSentinelMonitorName(clusterShard.getKey(), clusterShard.getValue());
	}

	@Override
	public ShardMeta getShardMeta(Long clusterDbId, Long shardDbId) {
		ClusterSummary clusterSummary = clusterDbIdMap.get(clusterDbId);
		if (null == clusterSummary || !clusterSummary.shards.containsKey(shardDbId)) return null;
		return getShardMeta(clusterSummary.name, clusterSummary.shards.get(shardDbId));
	}

	@Override
	public List<KeeperMeta> getKeepers(Long clusterDbId, Long shardDbId) {
		Pair<String, String> clusterShard = clusterShardDbId2Name(clusterDbId, shardDbId);
		return getKeepers(clusterShard.getKey(), clusterShard.getValue());
	}

	@Override
	public List<RedisMeta> getRedises(Long clusterDbId, Long shardDbId) {
		Pair<String, String> clusterShard = clusterShardDbId2Name(clusterDbId, shardDbId);
		return getRedises(clusterShard.getKey(), clusterShard.getValue());
	}

	@Override
	public KeeperMeta getKeeperActive(Long clusterDbId, Long shardDbId) {
		Pair<String, String> clusterShard = clusterShardDbId2Name(clusterDbId, shardDbId);
		return getKeeperActive(clusterShard.getKey(), clusterShard.getValue());
	}

	@Override
	public List<KeeperMeta> getKeeperBackup(Long clusterDbId, Long shardDbId) {
		Pair<String, String> clusterShard = clusterShardDbId2Name(clusterDbId, shardDbId);
		return getKeeperBackup(clusterShard.getKey(), clusterShard.getValue());
	}

	@Override
	public RedisMeta getRedisMaster(Long clusterDbId, Long shardDbId) {
		Pair<String, String> clusterShard = clusterShardDbId2Name(clusterDbId, shardDbId);
		return getRedisMaster(clusterShard.getKey(), clusterShard.getValue());
	}

	@Override
	public List<KeeperMeta> getAllSurviveKeepers(Long clusterDbId, Long shardDbId) {
		Pair<String, String> clusterShard = clusterShardDbId2Name(clusterDbId, shardDbId);
		return getAllSurviveKeepers(clusterShard.getKey(), clusterShard.getValue());
	}

	@Override
	public ClusterMeta removeCluster(Long clusterDbId) {
		return removeCluster(clusterDbId2Name(clusterDbId));
	}

	@Override
	public boolean updateKeeperActive(Long clusterDbId, Long shardDbId, KeeperMeta activeKeeper) {
		Pair<String, String> clusterShard = clusterShardDbId2Name(clusterDbId, shardDbId);
		return updateKeeperActive(clusterShard.getKey(), clusterShard.getValue(), activeKeeper);
	}

	@Override
	public boolean noneKeeperActive(Long clusterDbId, Long shardDbId) {
		Pair<String, String> clusterShard = clusterShardDbId2Name(clusterDbId, shardDbId);
		return noneKeeperActive(clusterShard.getKey(), clusterShard.getValue());
	}

	@Override
	public boolean updateRedisMaster(Long clusterDbId, Long shardDbId, RedisMeta redisMaster) {
		Pair<String, String> clusterShard = clusterShardDbId2Name(clusterDbId, shardDbId);
		return updateRedisMaster(clusterShard.getKey(), clusterShard.getValue(), redisMaster);
	}

	@Override
	public void setSurviveKeepers(Long clusterDbId, Long shardDbId, List<KeeperMeta> surviveKeepers) {
		Pair<String, String> clusterShard = clusterShardDbId2Name(clusterDbId, shardDbId);
		setSurviveKeepers(clusterShard.getKey(), clusterShard.getValue(), surviveKeepers);
	}

	@Override
	public Set<String> getBackupDcs(Long clusterDbId, Long shardDbId) {
		Pair<String, String> clusterShard = clusterShardDbId2Name(clusterDbId, shardDbId);
		return getBackupDcs(clusterShard.getKey(), clusterShard.getValue());
	}

	@Override
	public Set<String> getRelatedDcs(Long clusterDbId, Long shardDbId) {
		Pair<String, String> clusterShard = clusterShardDbId2Name(clusterDbId, shardDbId);
		return getRelatedDcs(clusterShard.getKey(), clusterShard.getValue());
	}

	@Override
	public void primaryDcChanged(Long clusterDbId, Long shardDbId, String newPrimaryDc) {
		Pair<String, String> clusterShard = clusterShardDbId2Name(clusterDbId, shardDbId);
		primaryDcChanged(clusterShard.getKey(), clusterShard.getValue(), newPrimaryDc);
	}
}

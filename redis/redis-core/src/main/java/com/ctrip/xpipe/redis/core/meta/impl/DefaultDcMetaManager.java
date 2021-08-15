package com.ctrip.xpipe.redis.core.meta.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.DcMetaManager;
import com.ctrip.xpipe.redis.core.meta.MetaClone;
import com.ctrip.xpipe.redis.core.meta.MetaException;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
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
	
	private DefaultDcMetaManager(String currentDc, XpipeMetaManager xpipeMetaManager){
		this.metaManager = xpipeMetaManager;
		this.currentDc = currentDc;
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
		return metaManager.getShardMeta(shardId, clusterId, shardId);
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
	public Set<String> getClusters() {
		return metaManager.getDcClusters(currentDc);
	}

	@Override
	public RouteMeta randomRoute(String clusterId) {

		ClusterMeta clusterMeta = metaManager.getClusterMeta(currentDc, clusterId);
		if(clusterMeta == null){
			throw new IllegalArgumentException("clusterId not exist:" + clusterId);
		}
		return metaManager.metaRandomRoutes(currentDc, clusterMeta.getOrgId(), clusterMeta.getActiveDc());
	}

	@Override
	public List<RouteMeta> getAllRoutes() {
		return metaManager.metaRoutes(currentDc);
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


}

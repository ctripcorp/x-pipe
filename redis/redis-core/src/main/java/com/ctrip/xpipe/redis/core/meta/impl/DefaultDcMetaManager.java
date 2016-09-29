package com.ctrip.xpipe.redis.core.meta.impl;

import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.MetaServerMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.entity.ZkServerMeta;
import com.ctrip.xpipe.redis.core.meta.DcMetaManager;
import com.ctrip.xpipe.redis.core.meta.MetaClone;
import com.ctrip.xpipe.redis.core.meta.MetaException;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
public class DefaultDcMetaManager implements DcMetaManager{
	
	protected XpipeMetaManager metaManager;
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	protected String currentDc;;
	
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
	public String getUpstream(String clusterId, String shardId) throws MetaException {
		return metaManager.getUpstream(currentDc, clusterId, shardId);
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
		return metaManager.getAllSurviceKeepers(currentDc, clusterId, shardId);
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
	public void updateUpstream(String clusterId, String shardId, String ip, int port) {
		metaManager.updateUpstream(currentDc, clusterId, shardId, ip, port);
	}


	@Override
	public String getActiveDc(String clusterId) {
		return metaManager.getActiveDc(clusterId);
	}


}

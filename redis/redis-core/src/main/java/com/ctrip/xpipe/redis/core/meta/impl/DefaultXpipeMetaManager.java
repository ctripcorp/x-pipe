package com.ctrip.xpipe.redis.core.meta.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.meta.MetaClone;
import com.ctrip.xpipe.redis.core.meta.MetaException;
import com.ctrip.xpipe.redis.core.meta.MetaUtils;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.redis.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.redis.core.util.OrgUtil;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.FileUtils;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.base.Joiner;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.*;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
public class DefaultXpipeMetaManager extends AbstractMetaManager implements XpipeMetaManager{
	
	private String fileName = null;

	protected final XpipeMeta xpipeMeta;
	private Map<HostPort, MetaDesc> inverseMap;
	
	public DefaultXpipeMetaManager(XpipeMeta xpipeMeta){
		this.xpipeMeta = xpipeMeta;
	}

	private DefaultXpipeMetaManager(String fileName) {
		this.fileName = fileName;
		xpipeMeta = load(fileName);
	}

	public static XpipeMetaManager buildFromFile(String fileName){
		return new DefaultXpipeMetaManager(fileName);
	}

	public static XpipeMetaManager buildFromMeta(XpipeMeta xpipeMeta){
		return new DefaultXpipeMetaManager(xpipeMeta);
	}

	public XpipeMeta load(String fileName) {
		
		try {
			InputStream ins = FileUtils.getFileInputStream(fileName);
			return DefaultSaxParser.parse(ins);
		} catch (SAXException | IOException e) {
			logger.error("[load]" + fileName, e);
			throw new IllegalStateException("load " + fileName + " failed!", e);
		}
	}
	
	@Override
	public String getActiveDc(String clusterId, String shardId){
		
		for(DcMeta dcMeta : xpipeMeta.getDcs().values()){
			ClusterMeta clusterMeta = dcMeta.getClusters().get(clusterId);
			if(clusterMeta == null){
				continue;
			}
			String activeDc = clusterMeta.getActiveDc();
			if(activeDc == null){
				logger.info("[getActiveDc][activeDc null]{}", clusterMeta);
				throw new MetaException(String.format("cluster exist but active dc == null {}", clusterMeta));
			}
			return activeDc.trim().toLowerCase();
		}
		throw new MetaException("clusterId " + clusterId + " not found!");
	}
	
	@Override
	public Set<String> getBackupDcs(String clusterId, String shardId) {

		boolean found = false;
		
		for(DcMeta dcMeta : xpipeMeta.getDcs().values()){
			ClusterMeta clusterMeta = dcMeta.getClusters().get(clusterId);
			if(clusterMeta == null){
				continue;
			}
			
			found = true;
			
			if(StringUtil.isEmpty(clusterMeta.getBackupDcs())){
				logger.info("[getBackupDcs][backup dcs empty]{}, {}", dcMeta.getId(), clusterMeta);
				continue;
			}
			
			
			Set<String> backDcs = backupDcs(clusterMeta.getBackupDcs());
			backDcs.remove(clusterMeta.getActiveDc().toLowerCase().trim());
			return backDcs;
		}
		
		if(found){
			return new HashSet<>();
		}
		throw new MetaException("clusterId " + clusterId + " not found!");
	}

	
	private Set<String> backupDcs(String backupDcsDesc) {
		
		Set<String> backDcs = new HashSet<>();
		if(StringUtil.isEmpty(backupDcsDesc)){
			return backDcs;
		}
		for(String dc : backupDcsDesc.split("\\s*,\\s*")){
			dc = dc.trim();
			if(!StringUtil.isEmpty(dc)){
				backDcs.add(dc.toLowerCase());
			}
		}
		return backDcs;
	}

	@Override
	public Set<String> getDcClusters(String dc) {
		return new HashSet<>(getDirectDcMeta(dc).getClusters().keySet());
	}

	@Override
	public ClusterMeta getClusterMeta(String dc, String clusterId) {
		
		return clone(getDirectClusterMeta(dc, clusterId));
	}
	
	public ClusterMeta getDirectClusterMeta(String dc, String clusterId) {
		
		DcMeta dcMeta = getDirectDcMeta(dc);
		if(dcMeta == null){
			return null;
		}
		return dcMeta.getClusters().get(clusterId);
	}
	
	protected DcMeta getDirectDcMeta(String dc) {

		for(Map.Entry<String, DcMeta> dentry : xpipeMeta.getDcs().entrySet()){
			String dcId = dentry.getKey();
			if(dcId.equalsIgnoreCase(dc)){
				return dentry.getValue();
			}
		}
		return null;
	}

	@Override
	public ShardMeta getShardMeta(String dc, String clusterId, String shardId) {
		
		return clone(getDirectShardMeta(dc, clusterId, shardId));
	}

	protected ShardMeta getDirectShardMeta(String dc, String clusterId, String shardId) {
		
		ClusterMeta clusterMeta = getDirectClusterMeta(dc, clusterId);
		if(clusterMeta == null){
			return null;
		}
		return clusterMeta.getShards().get(shardId);
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<KeeperMeta> getKeepers(String dc, String clusterId, String shardId) {
		
		return (List<KeeperMeta>) clone((Serializable)getDirectKeepers(dc, clusterId, shardId));
	}

	protected List<KeeperMeta> getDirectKeepers(String dc, String clusterId, String shardId) {
		
		ShardMeta shardMeta = getDirectShardMeta(dc, clusterId, shardId);
		if(shardMeta == null){
			return null;
		}
		return shardMeta.getKeepers();
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<RedisMeta> getRedises(String dc, String clusterId, String shardId) {
		
		ShardMeta shardMeta = getShardMeta(dc, clusterId, shardId);
		if(shardMeta == null){
			return null;
		}
		return (List<RedisMeta>) clone((Serializable)shardMeta.getRedises());
	}

	@Override
	public KeeperMeta getKeeperActive(String dc, String clusterId, String shardId) {
		
		List<KeeperMeta> keepers = getDirectKeepers(dc, clusterId, shardId);
		if(keepers == null){
			return null;
		}
		
		return clone(getDirectKeeperActive(keepers));
	}

	private KeeperMeta getDirectKeeperActive(List<KeeperMeta> keepers) {
		
		for(KeeperMeta keeperMeta : keepers){
			if(keeperMeta.isActive()){
				return keeperMeta;
			}
		}
		return null;
	}

	@Override
	public List<KeeperMeta> getKeeperBackup(String dc, String clusterId, String shardId) {
		
		List<KeeperMeta> keepers = getKeepers(dc, clusterId, shardId);
		if(keepers == null){
			return null;
		}
		
		LinkedList<KeeperMeta> result = new LinkedList<>();
		for(KeeperMeta keeperMeta : keepers){
			if(!keeperMeta.isActive()){
				result.add(keeperMeta);
			}
		}
		return clone(result);
	}

	@Override
	public MetaDesc findMetaDesc(HostPort hostPort) {

		if(inverseMap != null){
			return inverseMap.get(hostPort);
		}

		synchronized (this){
			if(inverseMap == null){
				inverseMap = InverseHostPortMapBuilder.build(xpipeMeta);
			}
		}

		return inverseMap.get(hostPort);
	}

	private ShardMeta cloneWithParent(DcMeta dcMeta, ClusterMeta clusterMeta, ShardMeta shardMeta) {

		ShardMeta result = clone(shardMeta);
		result.setParent(clone(clusterMeta));
		result.parent().setParent(clone(dcMeta));
		return result;
	}

	@Override
	public Pair<String, RedisMeta> getRedisMaster(String clusterId, String shardId) {
		
		for(DcMeta dcMeta : xpipeMeta.getDcs().values()){

			ClusterMeta clusterMeta = dcMeta.findCluster(clusterId);
			if( clusterMeta == null ){
				continue;
			}

			ShardMeta shardMeta = clusterMeta.findShard(shardId);
			if(shardMeta == null){
				continue;
			}

			for(RedisMeta redisMeta : shardMeta.getRedises()){
				if(redisMeta.isMaster()){
					return new Pair<>(dcMeta.getId(), clone(redisMeta));
				}
			}
		}
		return null;
	}

	
	
	@Override
	public boolean noneKeeperActive(String dc, String clusterId, String shardId) {
		
		ShardMeta shardMeta = getDirectShardMeta(dc, clusterId, shardId);
		
		if(shardMeta == null){
			throw new RedisRuntimeException(String.format("[shard not found]dc:%s, cluster:%s, shard:%s", dc, clusterId, shardId));
		}
		boolean changed = false;
		for(KeeperMeta keeperMeta : shardMeta.getKeepers()){
			if(keeperMeta.isActive()){
				keeperMeta.setActive(false);
				changed = true;
			}
		}
		return changed;
	}
	
	@Override
	public void setSurviveKeepers(String dcId, String clusterId, String shardId, List<KeeperMeta> surviveKeepers) {
		
		List<KeeperMeta> keepers = getDirectKeepers(dcId, clusterId, shardId);
		
		List<KeeperMeta> unfoundKeepers = new LinkedList<>();
		
		for(KeeperMeta active : surviveKeepers){
			boolean found = false;
			for(KeeperMeta current :keepers){
				if(MetaUtils.same(active, current)){
					found = true;
					current.setSurvive(true);
					break;
				}
			}
			if(!found){
				unfoundKeepers.add(active);
			}
		}
		
		if(unfoundKeepers.size() > 0){
			throw new IllegalArgumentException("unfound keeper set active:" + unfoundKeepers);
		}
		
	}

	@Override
	public boolean updateKeeperActive(String dc, String clusterId, String shardId, KeeperMeta activeKeeper) {
		
		if(!valid(activeKeeper)){
			logger.info("[updateKeeperActive][keeper information unvalid]{}", activeKeeper);
		}
		
		ShardMeta shardMeta = getDirectShardMeta(dc, clusterId, shardId);
		if(shardMeta == null){
			throw new MetaException(String.format("unfound keepers: %s %s %s", dc, clusterId, shardId));
		}
		List<KeeperMeta> keepers = shardMeta.getKeepers();
		boolean found = false;
		boolean changed = false;
		for(KeeperMeta keeperMeta : keepers){
			if(keeperMeta.getIp().equals(activeKeeper.getIp()) && keeperMeta.getPort().equals(activeKeeper.getPort())){
				found = true;
				if(!keeperMeta.isActive()){
					logger.info("[updateKeeperActive][set keeper active]{}", keeperMeta);
					keeperMeta.setActive(true);
					changed = true;
				}
			}else{
				if(keeperMeta.isActive()){
					logger.info("[updateKeeperActive][set keeper unactive]{}", keeperMeta);
					keeperMeta.setActive(false);
					changed = true;
				}
			}
		}
		if(!found && valid(activeKeeper)){
			changed = true;
			activeKeeper.setActive(true);
			activeKeeper.setParent(shardMeta);
			keepers.add(activeKeeper);
		}
		return changed;
	}
	
	private boolean valid(KeeperMeta activeKeeper) {
		
		if(activeKeeper == null || activeKeeper.getIp() == null || activeKeeper.getPort() == null){
			return false;
		}
		return true;
	}

	public String getFileName() {
		return fileName;
	}

	@Override
	public List<MetaServerMeta> getMetaServers(String dc) {
		
		DcMeta dcMeta = getDirectDcMeta(dc);
		if( dcMeta == null ){
			return null;
		}
		return clone(new LinkedList<>(dcMeta.getMetaServers()));
	}

	@Override
	public ZkServerMeta getZkServerMeta(String dc) {
		
		DcMeta dcMeta = getDirectDcMeta(dc);
		if( dcMeta == null ){
			return null;
		}
		return clone(dcMeta.getZkServer());
	}

	@Override
	public Set<String> getDcs() {
		return xpipeMeta.getDcs().keySet();
	}

	@Override
	public boolean updateRedisMaster(String dc, String clusterId, String shardId, RedisMeta redisMaster) throws MetaException {
		
		String activeDc = getActiveDc(clusterId, shardId);
		if(!activeDc.equals(dc)){
			throw new MetaException("active dc:" + activeDc + ", but given:" + dc + ", clusterID:" + clusterId);
		}
		ShardMeta shardMeta = getDirectShardMeta(dc, clusterId, shardId);
		if(shardMeta == null){
			throw new MetaException(String.format("unfound shard %s,%s,%s", dc, clusterId, shardId));
		}
		
		boolean found = false, changed = false;
		String newMaster = String.format("%s:%d", redisMaster.getIp(), redisMaster.getPort());
		String oldRedisMaster = null;
		for(RedisMeta redisMeta : shardMeta.getRedises()){
			if(redisMeta.getIp().equals(redisMaster.getIp()) && redisMeta.getPort().equals(redisMaster.getPort())){
				found = true;
				if(!redisMeta.isMaster()){
					logger.info("[updateRedisMaster][change redis to master]{}", redisMeta);
					redisMeta.setMaster(null);
					changed = true;
				}else{
					logger.info("[updateRedisMaster][redis already master]{}", redisMeta);
				}
			}else{
				if(redisMeta.isMaster()){
					logger.info("[updateRedisMaster][change redis to slave]{}", redisMeta);
					//unknown
					oldRedisMaster = String.format("%s:%d", redisMeta.getIp(), redisMeta.getPort());
					
					redisMeta.setMaster(getNewMasterofOldMaster(redisMeta, newMaster));
					changed = true;
				}
			}
		}
		
		
		if(oldRedisMaster != null){
			for(RedisMeta redisMeta : shardMeta.getRedises()){
				if(oldRedisMaster.equalsIgnoreCase(redisMeta.getMaster())){
					redisMeta.setMaster(newMaster);
					changed = true;
				}
			}
			for(KeeperMeta keeperMeta : shardMeta.getKeepers()){
				if(oldRedisMaster.equalsIgnoreCase(keeperMeta.getMaster())){
					keeperMeta.setMaster(newMaster);
					changed = true;
				}
			}
		}
		
		if(!found){
			redisMaster.setParent(shardMeta);
			shardMeta.getRedises().add(redisMaster);
			changed = true;
		}
		return changed;
	}

	private String getNewMasterofOldMaster(RedisMeta oldRedisMaster, String newRedisMaster) {
		
		Integer phase = oldRedisMaster.parent().getPhase(); 
		if(phase == null){
			phase = oldRedisMaster.parent().parent().getPhase();
		}
		if(phase == null){
			phase = 1;
		}
		
		if(phase == 1){
			return newRedisMaster;
		}
		KeeperMeta keeperActive = getDirectKeeperActive(oldRedisMaster.parent().getKeepers());
		if(keeperActive == null){
			throw new RedisRuntimeException(String.format("can not find active keeper:", oldRedisMaster.parent().getKeepers()));
		}
		return String.format("%s:%d", keeperActive.getIp(), keeperActive.getPort());
	}

	@Override
	public boolean dcExists(String dc) {
		return getDirectDcMeta(dc)!= null;
	}

	@Override
	public KeeperContainerMeta getKeeperContainer(String dc, KeeperMeta keeperMeta) {
		
		DcMeta dcMeta = getDirectDcMeta(dc);
		for(KeeperContainerMeta keeperContainerMeta : dcMeta.getKeeperContainers()){
			if(keeperContainerMeta.getId().equals(keeperMeta.getKeeperContainerId())){
				return clone(keeperContainerMeta);
			}
		}
		throw new IllegalArgumentException(String.format("[getKeeperContainer][unfound keepercontainer]%s, %s", dc, keeperMeta));
	}

	@Override
	public void update(DcMeta dcMeta) {
		
		xpipeMeta.addDc(clone(dcMeta));
	}

	@Override
	public void update(String dcId, ClusterMeta clusterMeta) {
		
		DcMeta dcMeta = xpipeMeta.getDcs().get(dcId);
		dcMeta.addCluster(clone(clusterMeta));
	}

	@Override
	public ClusterMeta removeCluster(String dcId, String clusterId) {
		
		DcMeta dcMeta = xpipeMeta.getDcs().get(dcId);
		return dcMeta.removeCluster(clusterId);
	}


	@Override
	public DcMeta getDcMeta(String dcId) {
		
		return clone(getDirectDcMeta(dcId));
	}

	@Override
	public List<KeeperMeta> getAllSurviceKeepers(String dcId, String clusterId, String shardId) {

		List<KeeperMeta> keepers = getDirectKeepers(dcId, clusterId, shardId);
		List<KeeperMeta> result = new LinkedList<>();
		
		for(KeeperMeta keeper : keepers){
			if(keeper.isSurvive()){
				result.add(MetaClone.clone(keeper));
			}
		}
		return result;
	}

	@Override
	public boolean hasCluster(String dcId, String clusterId) {
		DcMeta dcMeta = getDirectDcMeta(dcId);
		if(dcMeta == null){
			return false;
		}
		return dcMeta.getClusters().get(clusterId) != null;
	}

	@Override
	public boolean hasShard(String dcId, String clusterId, String shardId) {
		ShardMeta shardMeta = getDirectShardMeta(dcId, clusterId, shardId);
		if(shardMeta == null){
			return false;
		}
		return true;
	}

	@Override
	public SentinelMeta getSentinel(String dc, String clusterId, String shardId) {
		
		DcMeta dcMeta = getDirectDcMeta(dc);
		if((dcMeta == null)){
			throw new RedisRuntimeException("dcmeta not found:" + dc); 
		}
		ShardMeta shardMeta = getDirectShardMeta(dc, clusterId, shardId);
		if(shardMeta == null){
			throw new RedisRuntimeException(String.format("shardMeta not found:%s %s %s", dc, clusterId, shardId));
		}
		Long sentinelId = shardMeta.getSentinelId();
		SentinelMeta sentinelMeta = dcMeta.getSentinels().get(sentinelId);
		if(sentinelMeta == null){
			return new SentinelMeta().setAddress("");
		}
		return MetaClone.clone(sentinelMeta);
	}

	@Override
	public void primaryDcChanged(String dc, String clusterId, String shardId, String newPrimaryDc) {
		
		for(DcMeta dcMeta : xpipeMeta.getDcs().values()){
			changePrimaryDc(dcMeta, clusterId, shardId, newPrimaryDc);
		}
	}

	@Override
	public List<RouteMeta> routes(String currentDc, String tag) {

		DcMeta dcMeta = getDirectDcMeta(currentDc);
		List<RouteMeta> routes = dcMeta.getRoutes();
		List<RouteMeta> result = new LinkedList<>();

		if(routes != null){
			routes.forEach(routeMeta -> {
				if(routeMeta.tagEquals(tag) && currentDc.equalsIgnoreCase(routeMeta.getSrcDc())) {
					result.add(MetaClone.clone(routeMeta));
				}
			});
		}

		return result;
	}

	@Override
	public RouteMeta randomRoute(String currentDc, String tag, Integer orgId, String dstDc) {

		logger.info("[randomRoute]currentDc: {}, tag: {}, orgId: {}, dstDc: {}", currentDc, tag, orgId, dstDc);
		List<RouteMeta> routes = routes(currentDc, tag);
		if(routes == null || routes.isEmpty()){
			return null;
		}
		logger.info("[randomRoute]routes: {}", routes);
		//for Same dstdc
		List<RouteMeta> dstDcRoutes = new LinkedList<>();
		routes.forEach(routeMeta -> {
			if(routeMeta.getDstDc().equalsIgnoreCase(dstDc)){
				dstDcRoutes.add(routeMeta);
			}
		});
		if(dstDcRoutes.isEmpty()){
			logger.info("[randomRoute]dst dc empty: {}", routes);
			return null;
		}

		//for same org id
		List<RouteMeta> resultsCandidates = new LinkedList<>();
		dstDcRoutes.forEach(routeMeta -> {
			if(ObjectUtils.equals(routeMeta.getOrgId(), orgId)){
				resultsCandidates.add(routeMeta);
			}
		});

		if(!resultsCandidates.isEmpty()){
			return random(resultsCandidates);
		}


		dstDcRoutes.forEach(routeMeta -> {
			if(OrgUtil.isDefaultOrg(routeMeta.getOrgId())){
				resultsCandidates.add(routeMeta);
			}
		});

		return random(resultsCandidates);
	}

	@Override
	public List<ClusterMeta> getSpecificActiveDcClusters(String currentDc, String clusterActiveDc) {

		DcMeta directDcMeta = getDirectDcMeta(currentDc);
		if(directDcMeta == null){
			throw new IllegalArgumentException(String.format("can not find currentDc %s, %s", currentDc, clusterActiveDc));
		}

		List<ClusterMeta> result = new LinkedList<>();
		directDcMeta.getClusters().forEach((clusterId, clusterMeta) -> {
			if(clusterActiveDc.equalsIgnoreCase(clusterMeta.getActiveDc())){
				result.add(clone(clusterMeta));
			}
		});

		return result;
	}

	protected <T> T random(List<T> resultsCandidates) {

		if(resultsCandidates.isEmpty()){
			return null;
		}
		int random = new Random().nextInt(resultsCandidates.size());
		logger.info("[randomRoute]random: {}, size: {}", random, resultsCandidates.size());
		return resultsCandidates.get(random);

	}

	private void changePrimaryDc(DcMeta dcMeta, String clusterId, String shardId, String newPrimaryDc) {
		
		ClusterMeta clusterMeta = dcMeta.getClusters().get(clusterId);
		if(clusterMeta == null){
			throw new RedisRuntimeException(String.format("clusterMeta not found:%s %s %s", dcMeta.getId(), clusterId, shardId));
		}
		
		String currentPrimaryDc = clusterMeta.getActiveDc();
		
		if(StringUtil.trimEquals(newPrimaryDc, currentPrimaryDc, true)){
			logger.info("[changePrimaryDc][equal]{}, {}, {}", clusterId, shardId, newPrimaryDc);
			return;
		}
		
		newPrimaryDc = newPrimaryDc.trim().toLowerCase();
		
		Set<String> allDcs = new HashSet<>();
		if(currentPrimaryDc != null){
			allDcs.add(currentPrimaryDc.trim().toLowerCase());
		}
		allDcs.addAll(backupDcs(clusterMeta.getBackupDcs()));
		
		clusterMeta.setActiveDc(newPrimaryDc);
		
		allDcs.remove(newPrimaryDc);
		clusterMeta.setBackupDcs(Joiner.on(",").join(allDcs));
	}
	
	@Override
	public String toString() {
		return xpipeMeta.toString();
	}
}

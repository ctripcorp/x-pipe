package com.ctrip.xpipe.redis.meta.server.meta.impl;


import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.observer.NodeDeleted;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.DcMetaManager;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultDcMetaManager;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperElectorManager;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperStateController;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaServerMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

/**
 * @author wenchao.meng
 *
 * Aug 6, 2016
 */
@Component
public class DefaultCurrentMetaServerMetaManager extends AbstractLifecycleObservable implements CurrentMetaServerMetaManager{
	
	
	private int deadKeeperCheckIntervalMilli = 10000;
	
	@Autowired
	private SlotManager slotManager;
	
	@Autowired
	private CurrentClusterServer currentClusterServer;
	
	@Autowired
	private KeeperElectorManager keeperElectorManager;
	
	@Autowired
	private KeeperStateController keeperStateController;
	
	@Autowired
	private DcMetaCache dcMetaCache;
	
	private DcMetaManager currentServerMeta;
	
	private ScheduledExecutorService scheduled;
	private ScheduledFuture<?> scheduledFuture;
	
	
	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();
		
		currentServerMeta = DefaultDcMetaManager.buildForDc(FoundationService.DEFAULT.getDataCenter());
		scheduled = Executors.newScheduledThreadPool(2, XpipeThreadFactory.create(String.format("CHECK_DEAD_KEEPER(%d)", currentClusterServer.getServerId())));
	}
	
	
	@Override
	protected void doStart() throws Exception {
		super.doStart();
		
		Set<String> clusterIds = dcMetaCache.getClusters();

		for(String clusterId : clusterIds){
			
			if(currentClusterServer.hasKey(clusterId)){
				addCluster(clusterId);
			}
		}
		scheduledFuture = scheduled.scheduleWithFixedDelay(
				new DeadKeeperChecker(), 
				deadKeeperCheckIntervalMilli, 
				deadKeeperCheckIntervalMilli, 
				TimeUnit.MILLISECONDS);
	}

	
	@Override
	protected void doStop() throws Exception {
		
		if(scheduledFuture != null){
			scheduledFuture.cancel(true);
		}
		super.doStop();
	}
	
	
	@Override
	protected void doDispose() throws Exception {
		
		scheduled.shutdownNow();
		super.doDispose();
	}
	private void addCluster(String clusterId) {
		
		ClusterMeta clusterMeta = dcMetaCache.getClusterMeta(clusterId);
		
		logger.info("[addCluster]{}, {}", clusterId, clusterMeta);
		currentServerMeta.update(clusterMeta);
		
		notifyObservers(new NodeAdded<ClusterMeta>(clusterMeta));
	}

	private void removeCluster(String clusterId) {
		
		ClusterMeta clusterMeta = currentServerMeta.removeCluster(clusterId);
		logger.info("[removeCluster]{}, {}", clusterId, clusterMeta);
		if(clusterMeta != null){
			notifyObservers(new NodeDeleted<ClusterMeta>(clusterMeta));
		}
	}

	@Override
	public Set<String> allClusters() {
		return new HashSet<>(currentServerMeta.getClusters());
	}

	@Override
	public void deleteSlot(int slotId) {
		
		logger.info("[deleteSlot]{}", slotId);
		for(String clusterId : dcMetaCache.getClusters()){
			
			int currentSlotId = slotManager.getSlotIdByKey(clusterId);
			if(currentSlotId == slotId){
				removeCluster(clusterId);
			}
		}
	}


	@Override
	public void addSlot(int slotId) {
		
		logger.info("[addSlot]{}", slotId);
		for(String clusterId : dcMetaCache.getClusters()){
			
			int currentSlotId = slotManager.getSlotIdByKey(clusterId);
			if(currentSlotId == slotId){
				addCluster(clusterId);
			}
		}
	}

	@Override
	public void exportSlot(int slotId) {
		
		logger.info("[exportSlot]{}", slotId);
		deleteSlot(slotId);
	}

	@Override
	public void importSlot(int slotId) {
		
		logger.info("[importSlot][doNothing]{}", slotId);
	}

	public void keeperActiveElected(String clusterId, String shardId, KeeperMeta activeKeeper) throws Exception {
		
	}

	public void redisMasterChanged(String clusterId, String shardId, RedisMeta redisMaster) throws Exception {
		
	}

	@Override
	public DcMetaManager getCurrentMeta() {
		return currentServerMeta;
	}
	
	
	public class DeadKeeperChecker implements Runnable{

		@Override
		public void run() {
			
			try{
				doCheck();
			}catch(Throwable th){
				logger.error("[run]", th);
			}
			
		}
		
		private void doCheck() {
			
			for(String clusterId : currentServerMeta.getClusters()){
				ClusterMeta clusterMeta = currentServerMeta.getClusterMeta(clusterId);
				for(ShardMeta shardMeta : clusterMeta.getShards().values()){
					
					String shardId = shardMeta.getId();
					List<KeeperMeta> allKeepers = shardMeta.getKeepers();
					List<KeeperMeta> aliveKeepers = keeperElectorManager.getAllAliveKeepers(clusterId, shardId);
					List<KeeperMeta> deadKeepers = getDeadKeepers(allKeepers, aliveKeepers);
					
					if(deadKeepers.size() > 0){
						logger.info("[doCheck][dead keepers]{}", deadKeepers);
					}
					for(KeeperMeta deadKeeper : deadKeepers){
						try{
							keeperStateController.addKeeper(new KeeperTransMeta(clusterId, shardId, deadKeeper));
						}catch(ResourceAccessException e){
							logger.error(String.format("cluster:%s,shard:%s, keeper:%s, error:%s" , clusterId, shardId, deadKeeper, e.getMessage()));
						}catch(Throwable th){
							logger.error("[doCheck]", th);
						}
					}
				}
			}			
		}
	}

	protected List<KeeperMeta> getDeadKeepers(List<KeeperMeta> allKeepers, List<KeeperMeta> aliveKeepers) {
		
		List<KeeperMeta> result = new LinkedList<>();
		for(KeeperMeta allOne : allKeepers){
			boolean alive = false;
			for(KeeperMeta aliveOne : aliveKeepers){
				if(ObjectUtils.equals(aliveOne.getIp(), allOne.getIp()) && ObjectUtils.equals(aliveOne.getPort(), allOne.getPort())){
					alive = true;
					break;
				}
			}
			if(!alive){
				result.add(allOne);
			}
		}
		return result;
	}
}

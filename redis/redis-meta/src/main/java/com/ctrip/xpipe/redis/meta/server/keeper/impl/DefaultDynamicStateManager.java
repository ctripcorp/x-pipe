package com.ctrip.xpipe.redis.meta.server.keeper.impl;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.observer.NodeDeleted;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import com.ctrip.xpipe.redis.meta.server.keeper.DynamicStateManager;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperHeartBeatManager;
import com.ctrip.xpipe.utils.MapUtils;

import javax.annotation.Resource;

/**
 * @author wenchao.meng
 *
 * Aug 5, 2016
 */
@Component
public class DefaultDynamicStateManager implements DynamicStateManager{
	
	private Logger logger = LoggerFactory.getLogger(getClass());
		
	@Autowired
	private SlotManager slotManager;

	@Resource(name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
	private ScheduledExecutorService scheduled;
	
	private ConcurrentHashMap<KeeperKey, KeeperHeartBeatManager> keepers = new ConcurrentHashMap<>();
	
	private Map<String, ClusterMeta> clusterMetas = new ConcurrentHashMap<>();
	
	@Override
	public void ping(KeeperInstanceMeta keeperInstanceMeta) {

		final KeeperKey keeperKey = createKeeperKey(keeperInstanceMeta);

		logger.debug("[ping]{}", keeperKey);
		
		KeeperHeartBeatManager keeperHeartBeatManager = keepers.get(keeperKey);
		if(keeperHeartBeatManager == null){
			logger.error("[ping][error, not this cluster]{}", keeperKey);
			return;
		}
		keeperHeartBeatManager.ping(keeperInstanceMeta);
	}

	@Override
	public void update(Object args, Observable observable) {
		
		if(args instanceof NodeAdded<?>){
		
			//TODO keeper dead->active
			return;
		}
		if(args instanceof NodeDeleted<?>){

			//TODO keeper active->dead
			return;
		}
		
		//keeper master changed
		//keeper state changed
	}

	@Override
	public void add(ClusterMeta clusterMeta) {
		
		clusterMetas.put(clusterMeta.getId(), clusterMeta);
		
		for(ShardMeta shardMeta : clusterMeta.getShards().values()){
			for(KeeperMeta keeperMeta : shardMeta.getKeepers()){
				
				final KeeperKey keeperKey = createKeeperKey(clusterMeta.getId(), shardMeta.getId(), keeperMeta);
				@SuppressWarnings("unused")
				KeeperHeartBeatManager keeperHeartBeatManager = MapUtils.getOrCreate(keepers, keeperKey, new ObjectFactory<KeeperHeartBeatManager>() {
					@Override
					public KeeperHeartBeatManager create() {
						KeeperHeartBeatManager keeperHeartBeatManager = new DefaultKeeperHeartBeatManager(keeperKey, scheduled);
						keeperHeartBeatManager.addObserver(DefaultDynamicStateManager.this);
						return keeperHeartBeatManager;
					}
				});
			}
		}
	}

	@Override
	public void remove(String clusterId) {
		
		logger.info("[remote]{}", clusterId);
		
		clusterMetas.remove(clusterId);
		for(KeeperKey keeperKey : keepers.keySet()){
			if(keeperKey.getClusterId().equals(clusterId)){
				
				logger.info("[remove]{}", keeperKey);
				KeeperHeartBeatManager keeperHeartBeatManager = keepers.remove(keeperKey);
				keeperHeartBeatManager.close();
			}
		}
	}

	@Override
	public void removeBySlot(int slotId) {
		
		logger.info("[removeBySlot]{}", slotId);
		
		for(String clusterId : allClusters()){
			int clusterSlotId = slotManager.getSlotIdByKey(clusterId);
			if(clusterSlotId == slotId){
				remove(clusterId);
			}
		}
	}

	@Override
	public Set<String> allClusters() {
		
		Set<String> clusters = new HashSet<>();
		for(KeeperKey keeperKey : keepers.keySet()){
			clusters.add(keeperKey.getClusterId());
		}
		return clusters;
	}

	private KeeperKey createKeeperKey(KeeperInstanceMeta keeperInstanceMeta) {
		
		return createKeeperKey(keeperInstanceMeta.getClusterId(), 
				keeperInstanceMeta.getShardId(), 
				keeperInstanceMeta.getKeeperMeta());
	}

	private KeeperKey createKeeperKey(String clusterId, String shardId, KeeperMeta keeperMeta) {
		
		return new KeeperKey(
				clusterId, 
				shardId, 
				keeperMeta.getIp(), 
				keeperMeta.getPort());
	}
}

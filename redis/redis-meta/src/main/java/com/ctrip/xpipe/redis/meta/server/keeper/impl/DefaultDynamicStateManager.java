package com.ctrip.xpipe.redis.meta.server.keeper.impl;

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
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.utils.MapUtils;
import jakarta.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

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
	
	private Map<Long, ClusterMeta> clusterMetas = new ConcurrentHashMap<>();
	
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
		
		clusterMetas.put(clusterMeta.getDbId(), clusterMeta);
		
		for(ShardMeta shardMeta : clusterMeta.getAllShards().values()){
			for(KeeperMeta keeperMeta : shardMeta.getKeepers()){
				
				final KeeperKey keeperKey = createKeeperKey(clusterMeta.getDbId(), shardMeta.getDbId(), keeperMeta);
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
	public void remove(Long clusterDbId) {
		
		logger.info("[remote]cluster_{}", clusterDbId);
		
		clusterMetas.remove(clusterDbId);
		for(KeeperKey keeperKey : keepers.keySet()){
			if(keeperKey.getClusterDbId().equals(clusterDbId)){
				
				logger.info("[remove]{}", keeperKey);
				KeeperHeartBeatManager keeperHeartBeatManager = keepers.remove(keeperKey);
				keeperHeartBeatManager.close();
			}
		}
	}

	@Override
	public void removeBySlot(int slotId) {
		
		logger.info("[removeBySlot]{}", slotId);
		
		for(Long clusterDbId : allClusters()){
			int clusterSlotId = slotManager.getSlotIdByKey(clusterDbId);
			if(clusterSlotId == slotId){
				remove(clusterDbId);
			}
		}
	}

	@Override
	public Set<Long> allClusters() {
		
		Set<Long> clusters = new HashSet<>();
		for(KeeperKey keeperKey : keepers.keySet()){
			clusters.add(keeperKey.getClusterDbId());
		}
		return clusters;
	}

	private KeeperKey createKeeperKey(KeeperInstanceMeta keeperInstanceMeta) {
		
		return createKeeperKey(keeperInstanceMeta.getClusterDbId(),
				keeperInstanceMeta.getShardDbId(),
				keeperInstanceMeta.getKeeperMeta());
	}

	private KeeperKey createKeeperKey(Long clusterDbId, Long shardDbId, KeeperMeta keeperMeta) {
		
		return new KeeperKey(
				clusterDbId,
				shardDbId,
				keeperMeta.getIp(), 
				keeperMeta.getPort());
	}
}

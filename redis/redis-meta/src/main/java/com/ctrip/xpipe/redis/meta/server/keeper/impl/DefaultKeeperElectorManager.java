package com.ctrip.xpipe.redis.meta.server.keeper.impl;



import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.locks.LockInternals;
import org.apache.curator.framework.recipes.locks.LockInternalsSorter;
import org.apache.curator.framework.recipes.locks.StandardLockInternalsDriver;
import org.apache.zookeeper.WatchedEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperElectorManager;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperLeaderElectAlgorithm;
import com.ctrip.xpipe.zk.ZkClient;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
@Component
public class DefaultKeeperElectorManager extends AbstractCurrentMetaObserver implements KeeperElectorManager, Observer, TopElement{
	
	@Autowired
	private ZkClient zkClient;
	
	
	private void observeLeader(final ClusterMeta cluster) throws Exception {

		logger.info("[observeLeader]{}", cluster.getId());
		
		for (final ShardMeta shard : cluster.getShards().values()) {
			observerShardLeader(cluster.getId(), shard.getId());
		}
	}

	private void observerShardLeader(String clusterId, String shardId) {
		
		final String leaderLatchPath = MetaZkConfig.getKeeperLeaderLatchPath(clusterId, shardId);
		
		if(!currentMetaManager.watchIfNotWatched(clusterId, shardId)){
			logger.warn("[observeLeader][already watched]{}, {}", clusterId, shardId);
			return;
		}
		
		logger.info("[observerShardLeader]{}, {}", clusterId, shardId);
		final CuratorFramework client = zkClient.get();
		
		try {
			client.createContainers(leaderLatchPath);
			List<String> children = observeLeader(client, clusterId, shardId, leaderLatchPath);
			updateShardLeader(children, leaderLatchPath, clusterId, shardId);
		} catch (Exception e) {
			logger.error("[observerShardLeader]" + clusterId + "," + shardId, e);
		}
	}

	private List<String> observeLeader(final CuratorFramework client, final String clusterId, final String shardId, final String leaderLatchPath) throws Exception {

		if(!currentMetaManager.hasShard(clusterId, shardId)){
			logger.info("[observeLeader][clean watch]{}, {}", clusterId, shardId);
			return Collections.emptyList();
		}
		
		if(getLifecycleState().isDisposing() || getLifecycleState().isDisposed()){
			logger.info("[disposed clean watch]{}", clusterId);
			return Collections.emptyList();
		}
		
		logger.info("[observeLeader]({}){}, {}", currentClusterServer.getServerId(), clusterId, shardId);

		List<String> children = client.getChildren().usingWatcher(new CuratorWatcher() {

			@Override
			public void process(WatchedEvent event) throws Exception {
				
				observeLeader(client, clusterId, shardId, leaderLatchPath);
				
				if(getLifecycleState().isDisposing() || getLifecycleState().isDisposed()){
					logger.info("[process][dispose][stop]{},{}", clusterId, shardId);
					return;
				}
				logger.info("[process]{}, {}, {}",  event, this.hashCode(), currentClusterServer.getServerId());
				List<String> children = client.getChildren().forPath(leaderLatchPath);
				updateShardLeader(children, leaderLatchPath, clusterId, shardId);
			}
		}).forPath(leaderLatchPath);
		return children;

	}

	
	private LockInternalsSorter sorter = new LockInternalsSorter() {
		@Override
		public String fixForSorting(String str, String lockName) {
			return StandardLockInternalsDriver.standardFixForSorting(str, lockName);
		}
	};


	private void updateShardLeader(List<String> children, String leaderLatchPath, String clusterId, String shardId) throws Exception {

		logger.info("[updateShardLeader]{}, {}, {}, {}", children, leaderLatchPath, clusterId, shardId);

		List<KeeperMeta> surviveKeepers = new ArrayList<>(children.size());
		CuratorFramework client = zkClient.get();
		List<String> sortedChildren = LockInternals.getSortedChildren("latch-", sorter, children);
		
		for(String child : sortedChildren){
			String leaderId = new String(client.getData().forPath(leaderLatchPath + "/" + child));
			KeeperMeta keeper = Codec.DEFAULT.decode(leaderId, KeeperMeta.class);
			surviveKeepers.add(keeper);
		}
		
		KeeperLeaderElectAlgorithm klea = new DefaultLeaderElectAlgorithm();
		KeeperMeta activeKeeper = klea.select(surviveKeepers);
		

		currentMetaManager.setSurviveKeepers(clusterId, shardId, surviveKeepers, activeKeeper);
	}


	@Override
	protected void handleClusterModified(ClusterMetaComparator comparator) {
		
		String clusterId = comparator.getCurrent().getId();
				
		for(ShardMeta shardMeta : comparator.getAdded()){
			try {
				observerShardLeader(clusterId, shardMeta.getId());
			} catch (Exception e) {
				logger.error("[handleClusterModified]" + clusterId + "," + shardMeta.getId(), e);
			}
		}
	}

	@Override
	protected void handleClusterAdd(ClusterMeta clusterMeta) {
		try {
			observeLeader(clusterMeta);
		} catch (Exception e) {
			logger.error("[handleClusterAdd]" + clusterMeta.getId(), e);
		}
	}

	@Override
	protected void handleClusterDeleted(ClusterMeta clusterMeta) {
		//nothing to do
	}
}

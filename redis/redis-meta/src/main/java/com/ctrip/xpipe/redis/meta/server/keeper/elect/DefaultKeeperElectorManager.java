package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import java.util.ArrayList;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.locks.LockInternals;
import org.apache.curator.framework.recipes.locks.LockInternalsSorter;
import org.apache.curator.framework.recipes.locks.StandardLockInternalsDriver;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
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
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperActiveElectAlgorithm;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperActiveElectAlgorithmManager;
import com.ctrip.xpipe.redis.meta.server.keeper.impl.AbstractCurrentMetaObserver;
import com.ctrip.xpipe.zk.EternalWatcher;
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
	
	@Autowired
	private KeeperActiveElectAlgorithmManager keeperActiveElectAlgorithmManager;
	
	
	private void observeLeader(final ClusterMeta cluster) {

		logger.info("[observeLeader]{}", cluster.getId());
		
		for (final ShardMeta shard : cluster.getShards().values()) {
			observerShardLeader(cluster.getId(), shard.getId());
		}
	}

	protected void observerShardLeader(final String clusterId, final String shardId) {

		logger.info("[observerShardLeader]{}, {}", clusterId, shardId);

		final String leaderLatchPath = MetaZkConfig.getKeeperLeaderLatchPath(clusterId, shardId);
		final CuratorFramework client = zkClient.get();

		if(currentMetaManager.watchIfNotWatched(clusterId, shardId)){

			try {
				client.createContainers(leaderLatchPath);
				EternalWatcher eternalWatcher = new EternalWatcher(client, new CuratorWatcher() {

					@Override
					public void process(WatchedEvent event) throws Exception {

						logger.info("[process]{}, {}, {}",  event, this.hashCode(), currentClusterServer.getServerId());
						if(event.getType() != EventType.NodeChildrenChanged){
							logger.info("[process][event type not children changed, exit]");
							return;
						}

						List<String> children = client.getChildren().forPath(leaderLatchPath);
						updateShardLeader(children, leaderLatchPath, clusterId, shardId);
					}

					@Override
					public String toString() {
						return String.format("leader watcher %s,%s", clusterId, shardId);
					}
				}, leaderLatchPath);
				eternalWatcher.start();
				currentMetaManager.addResource(clusterId, shardId, eternalWatcher);
			} catch (Exception e) {
				logger.error("[observerShardLeader]" + clusterId + " " + shardId, e);
			}
		}else{
			logger.warn("[observerShardLeader][already watched]{}, {}", clusterId, shardId);
		}

		try {
			List<String> children = client.getChildren().forPath(leaderLatchPath);
			updateShardLeader(children, leaderLatchPath, clusterId, shardId);
		} catch (Exception e) {
			logger.error("[observerShardLeader]" + clusterId + "," + shardId, e);
		}
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
		
		
		KeeperActiveElectAlgorithm klea = keeperActiveElectAlgorithmManager.get(clusterId, shardId);
		KeeperMeta activeKeeper = klea.select(clusterId, shardId, surviveKeepers);
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
	
	public void setKeeperActiveElectAlgorithmManager(
			KeeperActiveElectAlgorithmManager keeperActiveElectAlgorithmManager) {
		this.keeperActiveElectAlgorithmManager = keeperActiveElectAlgorithmManager;
	}
}

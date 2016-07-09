package com.ctrip.xpipe.redis.meta.server.impl;

import java.util.List;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.locks.LockInternals;
import org.apache.curator.framework.recipes.locks.LockInternalsSorter;
import org.apache.curator.framework.recipes.locks.StandardLockInternalsDriver;
import org.apache.zookeeper.WatchedEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.zk.ZkClient;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
@Component
public class DefaultKeeperElectorManager extends AbstractLifecycleObservable implements KeeperElectorManager{
	
	@Autowired
	private ZkClient zkClient;
	
	@Autowired
	private MetaServer metaServer;
	
	@Override
	public List<KeeperMeta> getAllAliveKeepers(String clusterId, String shardId) {
		return null;
	}

	@Override
	public KeeperMeta getActive(String clusterId, String shardId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void observeCluster(ClusterMeta clusterMeta) throws Exception {
		observeLeader(clusterMeta);
	}


	private void observeLeader(final ClusterMeta cluster) throws Exception {

		logger.info("[observeLeader]{}", cluster.getId());
		
		for (final ShardMeta shard : cluster.getShards().values()) {
			
			final String leaderLatchPath = MetaZkConfig.getKeeperLeaderLatchPath(cluster.getId(), shard.getId());

			List<String> children = zkClient.get().getChildren().usingWatcher(new CuratorWatcher() {

				@Override
				public void process(WatchedEvent event) throws Exception {
					
					logger.info("[process]" + event);
					updateShardLeader(zkClient.get().getChildren().usingWatcher(this).forPath(leaderLatchPath), leaderLatchPath, cluster.getId(),
							shard.getId());
				}
			}).forPath(leaderLatchPath);

			updateShardLeader(children, leaderLatchPath, cluster.getId(), shard.getId());
		}
	}

	private LockInternalsSorter sorter = new LockInternalsSorter() {
		@Override
		public String fixForSorting(String str, String lockName) {
			return StandardLockInternalsDriver.standardFixForSorting(str, lockName);
		}
	};

	private void updateShardLeader(List<String> children, String leaderLatchPath, String clusterId, String shardId) throws Exception {
		
		KeeperMeta keeper = null;

		if (children != null && !children.isEmpty()) {
			List<String> sortedChildren = LockInternals.getSortedChildren("latch-", sorter, children);
			String leaderId = new String(zkClient.get().getData().forPath(leaderLatchPath + "/" + sortedChildren.get(0)));

			keeper = new KeeperMeta();
			keeper.setActive(true);
			// TODO
			String[] parts = leaderId.split(":");
			if (parts.length != 3) {
				throw new RuntimeException("Error leader data in zk: " + leaderId);
			}
			keeper.setIp(parts[0]);
			keeper.setPort(Integer.parseInt(parts[1]));
			keeper.setId(parts[2]);
		}

		metaServer.updateActiveKeeper(clusterId, shardId, keeper);
	}
}

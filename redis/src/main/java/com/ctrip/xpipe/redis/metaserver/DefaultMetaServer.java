package com.ctrip.xpipe.redis.metaserver;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.locks.LockInternals;
import org.apache.curator.framework.recipes.locks.LockInternalsSorter;
import org.apache.curator.framework.recipes.locks.StandardLockInternalsDriver;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.unidal.tuple.Pair;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.CoreConfig;
import com.ctrip.xpipe.redis.core.zk.ZkClient;
import com.ctrip.xpipe.redis.keeper.entity.ClusterMeta;
import com.ctrip.xpipe.redis.keeper.entity.DcMeta;
import com.ctrip.xpipe.redis.keeper.entity.KeeperMeta;
import com.ctrip.xpipe.redis.keeper.entity.RedisMeta;
import com.ctrip.xpipe.redis.keeper.entity.ShardMeta;
import com.ctrip.xpipe.redis.keeper.entity.XpipeMeta;
import com.ctrip.xpipe.redis.keeper.meta.ShardStatus;
import com.ctrip.xpipe.utils.ServicesUtil;

/**
 * @author marsqing
 *
 *         May 25, 2016 5:24:27 PM
 */
@Component
public class DefaultMetaServer extends AbstractLifecycle implements MetaServer {

	private static Logger log = LoggerFactory.getLogger(DefaultMetaServer.class);

	@Autowired
	MetaHolder metaHolder;

	@Autowired
	private CoreConfig config;

	@Autowired
	private ZkClient zkClient;

	private ConcurrentMap<Pair<String, String>, ShardStatus> shardStatuses = new ConcurrentHashMap<>();

	private FoundationService foundationService;
	
	@Override
	protected void doInitialize() throws Exception {
		metaHolder.initialize();
		zkClient.initialize();
	}

	@Override
	protected void doStart() throws Exception {
		
		zkClient.start();
		metaHolder.start();
		
		foundationService = ServicesUtil.getFoundationService();

		Map<String, ClusterMeta> clusters = metaHolder.getMeta().findDc(foundationService.getDataCenter()).getClusters();

		for (ClusterMeta cluster : clusters.values()) {
			for (ShardMeta shard : cluster.getShards().values()) {
				shardStatuses.put(new Pair<>(cluster.getId(), shard.getId()), new ShardStatus());
				updateRedisMaster(cluster.getId(), shard);
				updateUpstreamKeeper(cluster.getId(), shard.getId());
			}
		}

		// TODO watch and update cluster from zk
		for (ClusterMeta cluster : clusters.values()) {
			watchCluster(cluster);
		}
	}
	
	@Override
	protected void doStop() throws Exception {
		metaHolder.stop();
		zkClient.stop();
	}
	
	@Override
	protected void doDispose() throws Exception {
		
		metaHolder.dispose();;
		zkClient.dispose();;
	}

	@Override
	public KeeperMeta getActiveKeeper(String clusterId, String shardId) {
		ShardStatus status = shardStatuses.get(new Pair<>(clusterId, shardId));
		return status == null ? null : status.getActiveKeeper();
	}

	@Override
	public RedisMeta getRedisMaster(String clusterId, String shardId) {
		ShardStatus status = shardStatuses.get(new Pair<>(clusterId, shardId));
		return status == null ? null : status.getRedisMaster();
	}

	@Override
	public KeeperMeta getUpstreamKeeper(String clusterId, String shardId) {
		ShardStatus status = shardStatuses.get(new Pair<>(clusterId, shardId));
		return status == null ? null : status.getUpstreamKeeper();
	}

	@Override
	public void watchCluster(ClusterMeta cluster) throws Exception {
		observeLeader(cluster);
	}

	private void updateRedisMaster(String clusterId, ShardMeta shard) {
		ShardStatus shardStatus = shardStatuses.get(new Pair<>(clusterId, shard.getId()));

		if (shardStatus != null) {
			for (RedisMeta redis : shard.getRedises()) {
				if (redis.isMaster()) {
					shardStatus.setRedisMaster(redis);
				}
			}
		}
	}

	private void updateUpstreamKeeper(String clusterId, String shardId) {
		ShardStatus shardStatus = shardStatuses.get(new Pair<>(clusterId, shardId));

		if (shardStatuses != null) {
			XpipeMeta meta = metaHolder.getMeta();

			String thisDc = foundationService.getDataCenter();
			ShardMeta activeShard = null;
			for (DcMeta dc : meta.getDcs().values()) {
				if (thisDc.equals(dc.getId())) {
					// upstream keeper should locate in another dc
					continue;
				}

				ClusterMeta cluster = dc.findCluster(clusterId);
				if (cluster != null) {
					ShardMeta shard = cluster.findShard(shardId);
					if (shard != null && dc.getId().equals(shard.getActiveDc())) {
						activeShard = shard;
						break;
					}
				}
			}

			KeeperMeta upstreamKeeper = null;
			if (activeShard != null) {
				for (KeeperMeta keeper : activeShard.getKeepers()) {
					if (keeper.isActive()) {
						upstreamKeeper = keeper;
						break;
					}
				}
			}

			shardStatus.setUpstreamKeeper(upstreamKeeper);
		}
	}

	private void observeLeader(final ClusterMeta cluster) throws Exception {

		for (final ShardMeta shard : cluster.getShards().values()) {
			final String leaderLatchPath = String.format("%s/%s/%s", config.getZkLeaderLatchRootPath(), cluster.getId(), shard.getId());

			List<String> children = zkClient.get().getChildren().usingWatcher(new CuratorWatcher() {

				@Override
				public void process(WatchedEvent event) throws Exception {
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

		Pair<String, String> key = new Pair<>(clusterId, shardId);
		ShardStatus shardStatus = shardStatuses.get(key);
		if (shardStatus == null) {
			log.error("Unknown shard {} {}", clusterId, shardId);
			// TODO omit unknown shard?
		} else {
			if (keeper != null) {
				log.info("{} become active keeper of cluster:{} shard:{}", keeper, clusterId, shardId);
				shardStatus.setActiveKeeper(keeper);
			} else {
				log.info("all keeper of cluster:{} shard:{} is down", clusterId, shardId);
				shardStatus.setActiveKeeper(null);
			}
		}
	}

}

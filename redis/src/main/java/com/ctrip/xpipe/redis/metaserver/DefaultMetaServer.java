package com.ctrip.xpipe.redis.metaserver;


import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.PostConstruct;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.locks.LockInternals;
import org.apache.curator.framework.recipes.locks.LockInternalsSorter;
import org.apache.curator.framework.recipes.locks.StandardLockInternalsDriver;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.unidal.tuple.Pair;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.entity.ClusterMeta;
import com.ctrip.xpipe.redis.keeper.entity.KeeperMeta;
import com.ctrip.xpipe.redis.keeper.entity.RedisMeta;
import com.ctrip.xpipe.redis.keeper.entity.ShardMeta;
import com.ctrip.xpipe.redis.keeper.transform.DefaultSaxParser;

/**
 * @author marsqing
 *
 *         May 25, 2016 5:24:27 PM
 */
@Component
public class DefaultMetaServer implements MetaServer, Lifecycle {

	private static Logger log = LoggerFactory.getLogger(DefaultMetaServer.class);

	private ConcurrentMap<Pair<String, String>, Pair<KeeperMeta, RedisMeta>> shardState = new ConcurrentHashMap<>();

	private KeeperConfig config;

	private CuratorFramework client;

	public DefaultMetaServer() {
		// TODO
		config = new DefaultKeeperConfig();
	}

	@Override
	public void watchCluster(ClusterMeta cluster) throws Exception {
		observeLeader(cluster);
	}

	@Override
	public KeeperMeta getActiveKeeper(String clusterId, String shardId) {
		Pair<KeeperMeta, RedisMeta> state = shardState.get(new Pair<>(clusterId, shardId));
		return state == null ? null : state.getKey();
	}

	@Override
	public RedisMeta getRedisMaster(String clusterId, String shardId) {
		Pair<KeeperMeta, RedisMeta> state = shardState.get(new Pair<>(clusterId, shardId));
		return state == null ? null : state.getValue();
	}

	private void initializeZk() throws Exception {
		// TODO pass in client
		Builder builder = CuratorFrameworkFactory.builder();

		builder.connectionTimeoutMs(config.getZkConnectionTimeoutMillis());
		builder.connectString(config.getZkConnectionString());
		builder.maxCloseWaitMs(config.getZkCloseWaitMillis());
		builder.namespace(config.getZkNamespace());
		builder.retryPolicy(new RetryNTimes(config.getZkRetries(), config.getSleepMsBetweenRetries()));
		builder.sessionTimeoutMs(config.getZkSessionTimeoutMillis());

		client = builder.build();
		client.start();
		client.blockUntilConnected();
	}

	private void observeLeader(final ClusterMeta cluster) throws Exception {

		for (final ShardMeta shard : cluster.getShards()) {

			updateRedisMaster(cluster.getId(), shard);

			final String leaderLatchPath = String.format("%s/%s/%s", config.getZkLeaderLatchRootPath(), cluster.getId(), shard.getId());

			List<String> children = client.getChildren().usingWatcher(new CuratorWatcher() {

				@Override
				public void process(WatchedEvent event) throws Exception {
					updateShardLeader(client.getChildren().usingWatcher(this).forPath(leaderLatchPath), leaderLatchPath, cluster.getId(), shard.getId());
				}
			}).forPath(leaderLatchPath);

			updateShardLeader(children, leaderLatchPath, cluster.getId(), shard.getId());
		}
	}

	/**
	 * @param id
	 * @param shard
	 */
	private void updateRedisMaster(String clusterId, ShardMeta shard) {
		for (RedisMeta redis : shard.getRedises()) {
			if (redis.isMaster()) {
				shardState.put(new Pair<>(clusterId, shard.getId()), new Pair<>((KeeperMeta) null, redis));
			}
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
			String leaderId = new String(client.getData().forPath(leaderLatchPath + "/" + sortedChildren.get(0)));

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
		Pair<KeeperMeta, RedisMeta> state = shardState.get(key);
		if (state == null) {
			log.error("Unnown shard {} {}", clusterId, shardId);
			// TODO omit unknown shard?
		} else {
			if (keeper != null) {
				log.info("{} become active keeper of cluster:{} shard:{}", keeper, clusterId, shardId);
				state.setKey(keeper);
			} else {
				log.info("all keeper of cluster:{} shard:{} is down", clusterId, shardId);
				state.setKey(null);
			}
		}
	}

	@Override
	public void initialize() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	@PostConstruct
	public void start() throws Exception {
		initializeZk();
		// TODO
		InputStream ins = getClass().getClassLoader().getResourceAsStream("keeper6666.xml");
		ClusterMeta cluster = DefaultSaxParser.parse(ins).getClusters().get(0);
		watchCluster(cluster);
	}

	@Override
	public void stop() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void dispose() throws Exception {
		// TODO Auto-generated method stub

	}

}

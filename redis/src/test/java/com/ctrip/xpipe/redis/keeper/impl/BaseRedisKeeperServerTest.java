package com.ctrip.xpipe.redis.keeper.impl;

import java.io.File;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.retry.RetryNTimes;
import org.junit.Before;

import com.ctrip.xpipe.redis.core.CoreConfig;
import com.ctrip.xpipe.redis.core.DefaultCoreConfig;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.entity.ClusterMeta;
import com.ctrip.xpipe.redis.keeper.entity.KeeperMeta;
import com.ctrip.xpipe.redis.keeper.entity.ShardMeta;
import com.ctrip.xpipe.redis.keeper.entity.XpipeMeta;
import com.ctrip.xpipe.redis.keeper.transform.DefaultSaxParser;

/**
 * @author wenchao.meng
 *
 *         2016年4月21日 下午5:42:29
 */
public class BaseRedisKeeperServerTest extends AbstractRedisKeeperTest {

	@Before
	public void beforeDefaultRedisKeeperServerTest() throws Exception {
		initRegistry();
		startRegistry();
	}

	protected void startKeeper(String keeperConfigFile, String dc) throws Exception {

		XpipeMeta xpipe = DefaultSaxParser.parse(getClass().getClassLoader().getResourceAsStream(keeperConfigFile));
		ClusterMeta cluster = xpipe.findDc(dc).findCluster("cluster1");

		if (cluster == null) {
			throw new RuntimeException("wrong keeper config");
		}

		DefaultCoreConfig config = new DefaultCoreConfig();
		setupZkNodes(cluster, config);
		startKeepers(cluster);
	}

	private void startKeepers(final ClusterMeta cluster) throws Exception {

		for (final ShardMeta shard : cluster.getShards().values()) {

			int index = 0;
			for (final KeeperMeta keeper : shard.getKeepers()) {

				File storeDir = new File(getTestFileDir() + "/" + index);
				logger.info("[startKeepers]{},{},{}", cluster.getId(), shard.getId(), storeDir);
				ReplicationStoreManager replicationStoreManager = createReplicationStoreManager(cluster.getId(), shard.getId(), storeDir);
				createRedisKeeperServer(keeper, replicationStoreManager, metaServiceManager);
				index++;
			}
		}
	}

	private void setupZkNodes(ClusterMeta cluster, CoreConfig config) throws Exception {
		CuratorFramework client = initializeZK(config);
		for (ShardMeta shard : cluster.getShards().values()) {
			String path = String.format("%s/%s/%s", config.getZkLeaderLatchRootPath(), cluster.getId(), shard.getId());
			client.newNamespaceAwareEnsurePath(path).ensure(client.getZookeeperClient());
		}
	}

	private CuratorFramework initializeZK(CoreConfig config) throws InterruptedException {
		Builder builder = CuratorFrameworkFactory.builder();

		builder.connectionTimeoutMs(config.getZkConnectionTimeoutMillis());
		builder.connectString(config.getZkConnectionString());
		builder.maxCloseWaitMs(config.getZkCloseWaitMillis());
		builder.namespace(config.getZkNamespace());
		builder.retryPolicy(new RetryNTimes(3, config.getSleepMsBetweenRetries()));
		builder.sessionTimeoutMs(config.getZkSessionTimeoutMillis());

		CuratorFramework client = builder.build();
		client.start();
		client.blockUntilConnected();

		return client;
	}
}

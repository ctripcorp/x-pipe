package com.ctrip.xpipe.redis.keeper.impl;

import java.io.File;
import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.retry.RetryNTimes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
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
public class DefaultRedisKeeperServerTest extends AbstractRedisKeeperTest {

	@Before
	public void beforeDefaultRedisKeeperServerTest() throws Exception {
		initRegistry();
		startRegistry();
	}

	@Test
	public void startKeeper4444() throws Exception {

		startKeeper("keeper4444.xml", "oy");
	}

	@Test
	public void startKeeper5555() throws Exception {

		startKeeper("keeper5555.xml", "oy");
	}

	@Test
	public void startKeeper6666() throws Exception {

		startKeeper("keeper6666.xml", "jq");
	}

	@Test
	public void startKeeper7777() throws Exception {

		startKeeper("keeper7777.xml", "jq");
	}

	private void startKeeper(String keeperConfigFile, String dc) throws Exception {

		XpipeMeta xpipe = DefaultSaxParser.parse(getClass().getClassLoader().getResourceAsStream(keeperConfigFile));
		ClusterMeta cluster = xpipe.findDc(dc).findCluster("cluster1");

		if (cluster == null) {
			throw new RuntimeException("wrong keeper config");
		}

		DefaultKeeperConfig config = new DefaultKeeperConfig();
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
				createRedisKeeperServer(cluster.getId(), shard.getId(), keeper, replicationStoreManager, metaServiceManager);
				index++;
			}
		}
	}

	private void setupZkNodes(ClusterMeta cluster, KeeperConfig config) throws Exception {
		CuratorFramework client = initializeZK(config);
		for (ShardMeta shard : cluster.getShards().values()) {
			String path = String.format("%s/%s/%s", config.getZkLeaderLatchRootPath(), cluster.getId(), shard.getId());
			client.newNamespaceAwareEnsurePath(path).ensure(client.getZookeeperClient());
		}
	}

	private CuratorFramework initializeZK(KeeperConfig config) throws InterruptedException {
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

	@After
	public void afterOneBoxTest() throws IOException {

		System.out.println("Press any key to exit");
		System.in.read();
	}
}

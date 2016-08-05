package com.ctrip.xpipe.redis.keeper.impl;


import java.io.File;

import org.apache.curator.framework.CuratorFramework;
import org.junit.Before;

import com.ctrip.xpipe.redis.core.config.AbstractCoreConfig;
import com.ctrip.xpipe.redis.core.config.CoreConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.zk.impl.DefaultZkConfig;


/**
 * @author wenchao.meng
 *
 *         2016年4月21日 下午5:42:29
 */
public class BaseRedisKeeperServerTest extends AbstractRedisKeeperTest {

	@Before
	public void beforeDefaultRedisKeeperServerTest() throws Exception {
		startRegistry();
	}

	protected void startKeeper(String keeperConfigFile, String dc) throws Exception {

		XpipeMeta xpipe = DefaultSaxParser.parse(getClass().getClassLoader().getResourceAsStream(keeperConfigFile));
		ClusterMeta cluster = xpipe.findDc(dc).findCluster("cluster1");

		if (cluster == null) {
			throw new RuntimeException("wrong keeper config");
		}

		AbstractCoreConfig config = new AbstractCoreConfig();
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
				createRedisKeeperServer(keeper, replicationStoreManager, metaService);
				index++;
			}
		}
	}

	private void setupZkNodes(ClusterMeta cluster, CoreConfig config) throws Exception {
		CuratorFramework client = new DefaultZkConfig().create(config.getZkConnectionString());
		for (ShardMeta shard : cluster.getShards().values()) {
			String path = MetaZkConfig.getKeeperLeaderLatchPath(cluster.getId(), shard.getId());
			client.newNamespaceAwareEnsurePath(path).ensure(client.getZookeeperClient());
		}
	}

}

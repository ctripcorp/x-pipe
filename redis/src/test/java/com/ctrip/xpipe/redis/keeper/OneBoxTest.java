package com.ctrip.xpipe.redis.keeper;

import java.io.File;
import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.retry.RetryNTimes;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.keeper.cluster.ElectContext;
import com.ctrip.xpipe.redis.keeper.cluster.KeeperStarter;
import com.ctrip.xpipe.redis.keeper.cluster.LeaderElector;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.entity.Cluster;
import com.ctrip.xpipe.redis.keeper.entity.Keeper;
import com.ctrip.xpipe.redis.keeper.entity.Redis;
import com.ctrip.xpipe.redis.keeper.entity.Shard;
import com.ctrip.xpipe.redis.keeper.entity.Xpipe;
import com.ctrip.xpipe.redis.keeper.impl.DefaultKeeperMeta;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.impl.DefaultReplicationStore;
import com.ctrip.xpipe.redis.keeper.transform.DefaultSaxParser;
import com.ctrip.xpipe.redis.spring.KeeperContextConfig;

/**
 * @author marsqing
 *
 *         May 26, 2016 12:08:19 PM
 */
public class OneBoxTest {

	@Test
	public void startKeeper6666() throws Exception {
		Xpipe xpipe = DefaultSaxParser.parse(this.getClass().getResourceAsStream("keeper6666.xml"));
		Cluster cluster = xpipe.getClusters().get(0);

		DefaultKeeperConfig config = new DefaultKeeperConfig();

		setupZkNodes(cluster, config);
		File storeDir = new File(System.getProperty("user.home") + "/xpipetest/tmpdir");
		startKeepers(cluster, storeDir);

		System.out.println("Press any key to exit");
		System.in.read();
	}

	@Test
	public void startKeeper7777() throws Exception {
		
		Xpipe xpipe = DefaultSaxParser.parse(this.getClass().getResourceAsStream("keeper7777.xml"));
		Cluster cluster = xpipe.getClusters().get(0);

		DefaultKeeperConfig config = new DefaultKeeperConfig();

		setupZkNodes(cluster, config);
		File storeDir = new File(System.getProperty("user.home") + "/xpipetest/tmpdir2");
		startKeepers(cluster, storeDir);

		System.out.println("Press any key to exit");
		System.in.read();
	}

	/**
	 * @throws Exception
	 * 
	 */
	@SuppressWarnings("resource")
   private void startKeepers(final Cluster cluster, final File storeDir) throws Exception {
		final AnnotationConfigApplicationContext springCtx = new AnnotationConfigApplicationContext(
		      KeeperContextConfig.class);

		for (final Shard shard : cluster.getShards()) {
			KeeperConfig config = new DefaultKeeperConfig();

			String leaderElectionZKPath = String.format("%s/%s/%s", config.getZkLeaderLatchRootPath(), cluster.getId(),
			      shard.getId());
			final Redis master = findRedisMaster(shard);
			for (final Keeper keeper : shard.getKeepers()) {
				String leaderElectionID = String.format("%s:%s", keeper.getIp(), keeper.getPort());
				ElectContext ctx = new ElectContext(leaderElectionZKPath, leaderElectionID);
				LeaderElector elector = new LeaderElector(config, ctx);
				elector.start();

				new Thread() {

					private void startKeeper() throws Exception {
						System.out.println("Start keeper " + keeper);
						String keeperRunid = keeper.getId();
						Endpoint masterEndpoint = new DefaultEndPoint(String.format("redis://%s:%s", master.getIp(),
						      master.getPort()));

						ReplicationStore replicationStore = createReplicationStore(masterEndpoint, storeDir);

						DefaultRedisKeeperServer redisKeeperServer = new DefaultRedisKeeperServer(masterEndpoint,
						      replicationStore, new DefaultKeeperMeta(keeper.getPort(), keeperRunid, ""));
						redisKeeperServer.start();
					}

					public void run() {
						KeeperStarter keeperStarter = springCtx.getBean(KeeperStarter.class);
						System.out.println(String.format("keeper %s wait to become active", keeper));
						keeperStarter.waitUntilActive(cluster.getId(), shard.getId(), keeper);
						System.out.println(String.format("keeper %s become active", keeper));

						try {
							startKeeper();
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}.start();
			}
		}
	}

	private void setupZkNodes(Cluster cluster, KeeperConfig config) throws Exception {
		CuratorFramework client = initializeZK(config);
		for (Shard shard : cluster.getShards()) {
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

	private ReplicationStore createReplicationStore(Endpoint masterEndpoint, File storeDir) throws IOException {
		ReplicationStore replicationStore = new DefaultReplicationStore(storeDir, 1024 * 1024 * 1024);
		replicationStore.setMasterAddress(masterEndpoint);
		replicationStore.setKeeperBeginOffset(100);
		return replicationStore;
	}

	/**
	 * @param shard
	 * @return
	 */
	private Redis findRedisMaster(Shard shard) {
		for (Redis redis : shard.getRedises()) {
			if (redis.isMaster()) {
				return redis;
			}
		}
		return null;
	}

}

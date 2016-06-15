package com.ctrip.xpipe.redis.metaserver;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.Test;
import org.unidal.test.jetty.JettyServer;

import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;

/**
 * @author marsqing
 *
 *         May 30, 2016 3:26:13 PM
 */
public class StartMetaServer extends JettyServer {

	@Test
	public void start() throws Exception {
		startZk();
		setupZkNodes();

		startServer();

		System.out.println("Press any key to exit...");
		System.in.read();
	}

	@SuppressWarnings("resource")
	private void startZk() {
		try {
			new TestingServer(2181).start();
		} catch (Exception e) {
		}
	}

	private void setupZkNodes() throws Exception {
		KeeperConfig config = new DefaultKeeperConfig();
		CuratorFramework client = initializeZK(config);
		String path = String.format("%s/%s/%s", config.getZkLeaderLatchRootPath(), "cluster1", "shard1");
		client.newNamespaceAwareEnsurePath(path).ensure(client.getZookeeperClient());
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

	@Override
	protected String getContextPath() {
		return "/";
	}

	@Override
	protected int getServerPort() {
		return Integer.parseInt(System.getProperty("metaServerPort", "9747"));
	}

}

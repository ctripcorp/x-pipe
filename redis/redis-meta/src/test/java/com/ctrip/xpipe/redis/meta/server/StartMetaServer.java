package com.ctrip.xpipe.redis.meta.server;


import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Test;
import org.unidal.test.jetty.JettyServer;

import com.ctrip.xpipe.redis.core.foundation.IdcUtil;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.meta.server.dao.memory.MemoryMetaServerDao;



/**
 * @author marsqing
 *
 *         May 30, 2016 3:26:13 PM
 */
public class StartMetaServer extends JettyServer {

	private int zkPort = IdcUtil.JQ_ZK_PORT;
	private int serverPort = IdcUtil.JQ_METASERVER_PORT;

	@Test
	public void start9747() throws Exception {
		
		startZk();
		
		System.setProperty(MemoryMetaServerDao.MEMORY_META_SERVER_DAO_KEY, "metaserver--jq.xml");
		start(connectToZk("127.0.0.1:" + zkPort));
	}

	@Test
	public void start9748() throws Exception {
		
		this.zkPort = IdcUtil.OY_ZK_PORT;
		this.serverPort = IdcUtil.OY_METASERVER_PORT;

		System.setProperty(MemoryMetaServerDao.MEMORY_META_SERVER_DAO_KEY, "metaserver--oy.xml");

		IdcUtil.setToOY();
		startZk();
		start(connectToZk("127.0.0.1:" + zkPort));
	}


	public void start(CuratorFramework client) throws Exception {
		setupZkNodes(client);

		startServer();

	}

	@SuppressWarnings("resource")
	private void startZk() {
		try {
			new TestingServer(zkPort).start();
		} catch (Exception e) {
		}
	}

	private void setupZkNodes(CuratorFramework client) throws Exception {
		
		String path = MetaZkConfig.getKeeperLeaderLatchPath("cluster1", "shard1");
		client.newNamespaceAwareEnsurePath(path).ensure(client.getZookeeperClient());
		client.newNamespaceAwareEnsurePath("/meta").ensure(client.getZookeeperClient());
	}

	private CuratorFramework connectToZk(String connectString) throws InterruptedException {
		Builder builder = CuratorFrameworkFactory.builder();

		builder.connectionTimeoutMs(3000);
		builder.connectString(connectString);
		builder.maxCloseWaitMs(3000);
		builder.namespace("xpipe");
		builder.retryPolicy(new RetryNTimes(3, 1000));
		builder.sessionTimeoutMs(5000);

		CuratorFramework client = builder.build();
		client.start();
		client.blockUntilConnected();

		return client;
	}

	@Override
	protected String getContextPath() {
		return "/";
	}

	public int getZkPort() {
		return zkPort;
	}

	public void setZkPort(int zkPort) {
		this.zkPort = zkPort;
	}

	protected int getServerPort() {
		return serverPort;
	}

	public void setServerPort(int serverPort) {
		this.serverPort = serverPort;
	}

	@After
	public void afterStartMetaServer() throws IOException {
		System.out.println("Press any key to exit...");
		System.in.read();
	}

}

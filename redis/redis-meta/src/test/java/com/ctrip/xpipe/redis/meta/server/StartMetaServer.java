package com.ctrip.xpipe.redis.meta.server;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Test;
import org.unidal.helper.Files.IO;
import org.unidal.test.jetty.JettyServer;

import com.ctrip.xpipe.redis.core.CoreConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.foundation.IdcUtil;
import com.ctrip.xpipe.redis.core.impl.AbstractCoreConfig;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultMetaOperation;



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
		String meta = IO.INSTANCE.readFrom(getClass().getResourceAsStream("/metaserver--jq.xml"), "utf-8");
		start(connectToZk("127.0.0.1:" + zkPort), meta);
	}

	@Test
	public void start9748() throws Exception {
		this.zkPort = IdcUtil.OY_ZK_PORT;
		this.serverPort = IdcUtil.OY_METASERVER_PORT;
		
		IdcUtil.setToOY();
		
		startZk();

		String meta = IO.INSTANCE.readFrom(getClass().getResourceAsStream("/metaserver--oy.xml"), "utf-8");
		start(connectToZk("127.0.0.1:" + zkPort), meta);
	}

	public void start(DcMeta meta) throws Exception {
		start(connectToZk("127.0.0.1:" + zkPort), extractDcMeta(meta));
	}

	private String extractDcMeta(DcMeta meta) throws Exception {
		XpipeMeta xpipe = new XpipeMeta();
		DcMeta dc = new DcMeta();
		xpipe.addDc(dc);

		dc.setId(meta.getId());

		for (ClusterMeta cluster : meta.getClusters().values()) {
			ClusterMeta clusterClone = new ClusterMeta();
			dc.addCluster(clusterClone);

			clusterClone.setId(cluster.getId());

			for (ShardMeta shard : cluster.getShards().values()) {
				ShardMeta shardClone = new ShardMeta();
				clusterClone.addShard(shardClone);

				shardClone.setId(shard.getId());
				shardClone.setActiveDc(shard.getActiveDc());

				for (RedisMeta redis : shard.getRedises()) {
					RedisMeta redisClone = new RedisMeta();
					shardClone.addRedis(redisClone);

					redisClone.setIp(redis.getIp());
					redisClone.setMaster(redis.getMaster());
					redisClone.setPort(redis.getPort());
					redisClone.setShardActive(redis.getShardActive());

				}
			}
		}

		return xpipe.toString();
	}

	public void start(CuratorFramework client, String meta) throws Exception {
		setupZkNodes(client);
		new DefaultMetaOperation(client).update(meta);

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
		CoreConfig config = new AbstractCoreConfig();

		String path = String.format("%s/%s/%s", config.getZkLeaderLatchRootPath(), "cluster1", "shard1");
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

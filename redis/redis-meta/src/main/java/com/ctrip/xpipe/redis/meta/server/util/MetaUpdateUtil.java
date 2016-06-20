package com.ctrip.xpipe.redis.meta.server.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.retry.RetryNTimes;

import com.ctrip.xpipe.redis.meta.server.config.DefaultMetaServerConfig;


/**
 * @author wenchao.meng
 *
 * Jun 20, 2016
 */
public class MetaUpdateUtil {
	
	
	public static void updateMeta(CuratorFramework client, String meta) throws Exception {
		DefaultMetaServerConfig config = new DefaultMetaServerConfig();
		client.setData().forPath(config.getZkMetaStoragePath(), meta.getBytes());
	}

	public static void updateJQMeta(String meta, int zkPort) throws Exception {
		DefaultMetaServerConfig config = new DefaultMetaServerConfig();
		CuratorFramework client = connectToZk("127.0.0.1:" + zkPort);

		client.setData().forPath(config.getZkMetaStoragePath(), meta.getBytes());

		client.close();
	}

	public static void updateOYMeta(String meta, int zkPort) throws Exception {
		DefaultMetaServerConfig config = new DefaultMetaServerConfig();
		CuratorFramework client = connectToZk("127.0.0.1:" + zkPort);

		client.setData().forPath(config.getZkMetaStoragePath(), meta.getBytes());

		client.close();
	}

	private static CuratorFramework connectToZk(String connectString) throws Exception {
		Builder builder = CuratorFrameworkFactory.builder();

		builder.connectionTimeoutMs(3000);
		builder.connectString(connectString);
		builder.maxCloseWaitMs(3000);
		builder.namespace("xpipe");
		builder.retryPolicy(new RetryNTimes(1, 1000));
		builder.sessionTimeoutMs(5000);

		CuratorFramework client = builder.build();
		client.start();
		client.blockUntilConnected();

		return client;
	}

}

/**
 * 
 */
package com.ctrip.xpipe.redis.foundation;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.retry.RetryNTimes;

import com.ctrip.xpipe.redis.metaserver.config.DefaultMetaServerConfig;

/**
 * @author marsqing
 *
 *         Jun 16, 2016 2:08:06 PM
 */
public class IdcUtil {

	public final static int JQ_ZK_PORT = 2181;
	public final static int JQ_METASERVER_PORT = 9747;

	public final static int OY_ZK_PORT = 2182;
	public final static int OY_METASERVER_PORT = 9748;

	public static void setToJQ() {
		FakeFoundationService.setDataCenter("jq");
		System.setProperty("metaServerPort", JQ_METASERVER_PORT + "");
		System.setProperty("zkPort", JQ_ZK_PORT + "");
	}

	public static void setToOY() {
		FakeFoundationService.setDataCenter("oy");
		System.setProperty("metaServerPort", OY_METASERVER_PORT + "");
		System.setProperty("zkPort", OY_ZK_PORT + "");
	}

	public static void updateMeta(CuratorFramework client, String meta) throws Exception {
		DefaultMetaServerConfig config = new DefaultMetaServerConfig();
		client.setData().forPath(config.getZkMetaStoragePath(), meta.getBytes());
	}

	public static void updateJQMeta(String meta) throws Exception {
		DefaultMetaServerConfig config = new DefaultMetaServerConfig();
		CuratorFramework client = connectToZk("127.0.0.1:" + JQ_ZK_PORT);

		client.setData().forPath(config.getZkMetaStoragePath(), meta.getBytes());

		client.close();
	}

	public static void updateOYMeta(String meta) throws Exception {
		DefaultMetaServerConfig config = new DefaultMetaServerConfig();
		CuratorFramework client = connectToZk("127.0.0.1:" + OY_ZK_PORT);

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

/**
 * 
 */
package com.ctrip.xpipe.redis.core.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.retry.RetryNTimes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.CoreConfig;
import com.ctrip.xpipe.redis.util.XpipeThreadFactory;

/**
 * @author marsqing
 *
 *         Jun 16, 2016 12:05:57 PM
 */
@Component
public class DefaultZkClient extends AbstractLifecycle implements ZkClient {

	@Autowired
	CoreConfig config;

	private CuratorFramework client;

	@Override
	protected void doStart() throws Exception {
		Builder builder = CuratorFrameworkFactory.builder();

		builder.connectionTimeoutMs(config.getZkConnectionTimeoutMillis());
		builder.connectString(config.getZkConnectionString());
		builder.maxCloseWaitMs(config.getZkCloseWaitMillis());
		builder.namespace(config.getZkNamespace());
		builder.retryPolicy(new RetryNTimes(config.getZkRetries(), config.getSleepMsBetweenRetries()));
		builder.sessionTimeoutMs(config.getZkSessionTimeoutMillis());
		// TODO
		builder.threadFactory(XpipeThreadFactory.create("Xpipe-ZK", true));

		client = builder.build();
		client.start();
		client.blockUntilConnected();
	}

	@Override
	public CuratorFramework get() {
		return client;
	}

	// TODO remove when all code migrate to spring
	public void setConfig(CoreConfig config) {
		this.config = config;
	}

}

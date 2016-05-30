package com.ctrip.xpipe.redis.keeper.cluster;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.RetryNTimes;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.util.XpipeThreadFactory;

/**
 * @author marsqing
 *
 *         May 25, 2016 11:01:39 AM
 */
public class LeaderElector implements Lifecycle {

	private KeeperConfig config;

	private LeaderLatch latch;

	private ElectContext ctx;

	public LeaderElector(KeeperConfig config, ElectContext ctx) {
		this.config = config;
		this.ctx = ctx;
	}

	@Override
	public void initialize() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void start() throws Exception {
		CuratorFramework client = initializeZK();

		latch = new LeaderLatch(client, ctx.getLeaderElectionZKPath(), ctx.getLeaderElectionID());
		latch.addListener(new LeaderLatchListener() {

			@Override
			public void notLeader() {
			}

			@Override
			public void isLeader() {
			}
		});

		// TODO delay start until other parts of keeper is ready
		latch.start();
	}

	private CuratorFramework initializeZK() throws InterruptedException {
		Builder builder = CuratorFrameworkFactory.builder();

		builder.connectionTimeoutMs(config.getZkConnectionTimeoutMillis());
		builder.connectString(config.getZkConnectionString());
		builder.maxCloseWaitMs(config.getZkCloseWaitMillis());
		builder.namespace(config.getZkNamespace());
		builder.retryPolicy(new RetryNTimes(config.getZkRetries(), config.getSleepMsBetweenRetries()));
		builder.sessionTimeoutMs(config.getZkSessionTimeoutMillis());
		builder.threadFactory(XpipeThreadFactory.create("Keeper-ZK", true));

		CuratorFramework client = builder.build();
		client.start();
		client.blockUntilConnected();

		return client;
	}

	@Override
	public void stop() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void dispose() throws Exception {
		latch.close();
	}

}

package com.ctrip.xpipe.redis.keeper.cluster;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;

/**
 * @author marsqing
 *
 *         May 25, 2016 11:01:39 AM
 */
public class LeaderElector extends AbstractLifecycle implements Lifecycle {

	private LeaderLatch latch;

	private ElectContext ctx;

	private CuratorFramework zkClient;

	public LeaderElector(ElectContext ctx, CuratorFramework zkClient) {
		this.ctx = ctx;
		this.zkClient = zkClient;
	}

	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();
	}

	@Override
	public void doStart() throws Exception {
		super.doStart();

		latch = new LeaderLatch(zkClient, ctx.getLeaderElectionZKPath(), ctx.getLeaderElectionID());
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

	@Override
	public void doStop() throws Exception {
		super.doStop();
		if (latch != null) {
			latch.close();
		}

	}

	@Override
	public void doDispose() throws Exception {

		super.doDispose();
	}

}

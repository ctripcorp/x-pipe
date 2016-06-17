package com.ctrip.xpipe.redis.keeper.cluster;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;

/**
 * @author marsqing
 *
 *         May 25, 2016 11:01:39 AM
 */
public class DefaultLeaderElector extends AbstractLifecycle implements LeaderElector {

	private LeaderLatch latch;

	private ElectContext ctx;

	private CuratorFramework zkClient;

	public DefaultLeaderElector(ElectContext ctx, CuratorFramework zkClient) {
		this.ctx = ctx;
		this.zkClient = zkClient;
	}

	@Override
	protected void doStart() throws Exception {
		elect();
	}
	
	@Override
	public void elect() throws Exception {

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
		if (latch != null) {
			latch.close();
		}
	}

}

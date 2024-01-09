package com.ctrip.xpipe.cluster;

import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;

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
		
		zkClient.createContainers(ctx.getLeaderElectionZKPath());
		
		latch = new LeaderLatch(zkClient, ctx.getLeaderElectionZKPath(), ctx.getLeaderElectionID());
		latch.addListener(new LeaderLatchListener() {

			@Override
			public void notLeader() {
			}

			@Override
			public void isLeader() {
			}
		});

		latch.start();
		logger.info("[elect]{}", ctx);
	}

	@Override
	public boolean hasLeaderShip() {
		return null != latch && latch.hasLeadership();
	}

	@Override
	public void doStop() throws Exception {
		if (latch != null) {
			latch.close();
		}
	}

}

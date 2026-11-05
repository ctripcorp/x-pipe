package com.ctrip.xpipe.cluster;

import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;

import java.util.function.Supplier;

/**
 * @author marsqing
 *
 *         May 25, 2016 11:01:39 AM
 */
public class DefaultLeaderElector extends AbstractLifecycle implements LeaderElector {

	private LeaderLatch latch;

	/** Read on every start, so a stop/start round picks up a changed election id. */
	private Supplier<ElectContext> ctxSupplier;

	private CuratorFramework zkClient;

	public DefaultLeaderElector(ElectContext ctx, CuratorFramework zkClient) {
		this(() -> ctx, zkClient);
	}

	public DefaultLeaderElector(Supplier<ElectContext> ctxSupplier, CuratorFramework zkClient) {
		this.ctxSupplier = ctxSupplier;
		this.zkClient = zkClient;
	}

	@Override
	protected void doStart() throws Exception {
		elect();
	}

	@Override
	public void elect() throws Exception {

		ElectContext ctx = ctxSupplier.get();
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

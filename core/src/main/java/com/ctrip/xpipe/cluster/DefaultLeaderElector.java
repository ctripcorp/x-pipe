package com.ctrip.xpipe.cluster;

import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * @author marsqing
 *
 *         May 25, 2016 11:01:39 AM
 */
public class DefaultLeaderElector extends AbstractLifecycle implements LeaderElector {

	private static final long RECONNECT_REPUBLISH_DELAY_MILLI = 100L;

	private final Object latchLock = new Object();
	private volatile LeaderLatch latch;

	/** Read on every start and data publication, so the node carries the latest election id. */
	private final Supplier<ElectContext> ctxSupplier;

	private final CuratorFramework zkClient;

	/** True after this elector has used mutable node data at least once. */
	private volatile boolean mutableElectionData;
	private final AtomicBoolean sessionLost = new AtomicBoolean(false);
	private final AtomicBoolean reconnectRepublishScheduled = new AtomicBoolean(false);

	private final ConnectionStateListener connectionStateListener = (client, state) -> {
		if (state == ConnectionState.LOST && mutableElectionData) {
			sessionLost.set(true);
			return;
		}
		if (state == ConnectionState.RECONNECTED && sessionLost.get()) {
			scheduleReconnectRepublish();
		}
	};

	public DefaultLeaderElector(ElectContext ctx, CuratorFramework zkClient) {
		this(() -> ctx, zkClient);
	}

	public DefaultLeaderElector(Supplier<ElectContext> ctxSupplier, CuratorFramework zkClient) {
		this.ctxSupplier = ctxSupplier;
		this.zkClient = zkClient;
	}

	@Override
	protected void doStart() throws Exception {
		zkClient.getConnectionStateListenable().addListener(connectionStateListener);
		try {
			elect();
		} catch (Exception e) {
			zkClient.getConnectionStateListenable().removeListener(connectionStateListener);
			throw e;
		}
	}

	@Override
	public void elect() throws Exception {
		synchronized (latchLock) {
			ElectContext ctx = ctxSupplier.get();
			zkClient.createContainers(ctx.getLeaderElectionZKPath());

			LeaderLatch fresh = new LeaderLatch(zkClient, ctx.getLeaderElectionZKPath(), ctx.getLeaderElectionID());
			fresh.addListener(new LeaderLatchListener() {

				@Override
				public void notLeader() {
				}

				@Override
				public void isLeader() {
				}
			});

			latch = fresh;
			fresh.start();
			logger.info("[elect]{}", ctx);
		}
	}

	@Override
	public boolean hasLeaderShip() {
		LeaderLatch current = latch;
		return null != current && current.hasLeadership();
	}

	@Override
	public boolean publishElectionData() {
		synchronized (latchLock) {
			LeaderLatch current = latch;
			if (current == null || current.getState() != LeaderLatch.State.STARTED) {
				logger.info("[publishElectionData][latch not started]");
				return false;
			}

			String path = current.getOurPath();
			if (path == null || path.isEmpty()) {
				logger.info("[publishElectionData][own path not ready]");
				return false;
			}

			try {
				ElectContext ctx = ctxSupplier.get();
				byte[] expected = ctx.getLeaderElectionID().getBytes(StandardCharsets.UTF_8);
				Stat stat = new Stat();
				byte[] actual = zkClient.getData().storingStatIn(stat).forPath(path);
				if (!Arrays.equals(actual, expected)) {
					zkClient.setData().withVersion(stat.getVersion()).forPath(path, expected);
					logger.info("[publishElectionData][updated]{}", path);
				} else {
					logger.debug("[publishElectionData][unchanged]{}", path);
				}
				mutableElectionData = true;
				return true;
			} catch (Throwable th) {
				logger.info("[publishElectionData][fail]{}", path, th);
				return false;
			}
		}
	}

	private void scheduleReconnectRepublish() {
		if (!reconnectRepublishScheduled.compareAndSet(false, true)) {
			return;
		}
		try {
			RebuildableLeaderLatch.sharedProbeScheduler().schedule(
					this::republishAfterReconnect,
					RECONNECT_REPUBLISH_DELAY_MILLI,
					TimeUnit.MILLISECONDS);
		} catch (RejectedExecutionException e) {
			reconnectRepublishScheduled.set(false);
			logger.warn("[scheduleReconnectRepublish][rejected]", e);
		}
	}

	private void republishAfterReconnect() {
		reconnectRepublishScheduled.set(false);
		if (!sessionLost.get() || !getLifecycleState().isStarted()) {
			return;
		}
		if (publishElectionData()) {
			sessionLost.set(false);
			logger.info("[republishAfterReconnect][success]");
			return;
		}
		// LeaderLatch recreates its ephemeral node asynchronously after a new session.
		// Retry until its new ownPath is ready; the shared scheduler avoids one thread per shard.
		scheduleReconnectRepublish();
	}

	@Override
	public void doStop() throws Exception {
		zkClient.getConnectionStateListenable().removeListener(connectionStateListener);
		sessionLost.set(false);
		reconnectRepublishScheduled.set(false);
		synchronized (latchLock) {
			if (latch != null) {
				latch.close();
			}
		}
	}

}

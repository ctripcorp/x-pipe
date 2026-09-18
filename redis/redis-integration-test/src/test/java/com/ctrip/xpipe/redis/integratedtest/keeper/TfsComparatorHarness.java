package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.redis.comparator.compare.CompareLane;
import com.ctrip.xpipe.redis.comparator.compare.ShardComparator;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.comparator.meta.KeeperStreamFactory;
import com.ctrip.xpipe.redis.comparator.report.CompareReporter;
import com.ctrip.xpipe.redis.comparator.stream.StreamRingBuffer;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 进程内装配生产 Comparator（spec D37 / §4.12.3）。不起 Spring / Console / CMS。
 */
public final class TfsComparatorHarness {

	private static final Logger logger = LoggerFactory.getLogger(TfsComparatorHarness.class);

	private static final int EVENT_LOOP_SHUTDOWN_SECONDS = 3;

	private final long shardDbId;

	private final NioEventLoopGroup group;

	private final KeeperStreamFactory factory;

	private final ShardComparator comparator;

	private final List<CompareLane> lanes = new ArrayList<>(2);

	private final AtomicBoolean stopped = new AtomicBoolean();

	public TfsComparatorHarness(String cluster, String shard, long shardDbId, Endpoint active, Endpoint prepare,
			ScheduledExecutorService scheduled, CompareReporter reporter, ComparatorConfig config) {
		this.shardDbId = shardDbId;
		this.group = new NioEventLoopGroup(1, XpipeThreadFactory.create("tfs-cmp-" + cluster + "-" + shard, true));
		this.factory = new KeeperStreamFactory.Default(group, scheduled, config,
				KeeperStreamFactory.DEFAULT_LISTENING_PORT);
		this.comparator = new ShardComparator(cluster, shard, Collections.emptyList(), config, reporter);
		boolean started = false;
		try {
			comparator.start();
			lanes.add(factory.open(shardDbId, cluster, shard, active, comparator::wake));
			lanes.add(factory.open(shardDbId, cluster, shard, prepare, comparator::wake));
			comparator.replaceLanes(new ArrayList<>(lanes));
			started = true;
		} finally {
			if (!started) {
				stop();
			}
		}
	}

	@VisibleForTesting
	TfsComparatorHarness(long shardDbId, NioEventLoopGroup group, KeeperStreamFactory factory,
			ShardComparator comparator, List<CompareLane> lanes) {
		this.shardDbId = shardDbId;
		this.group = group;
		this.factory = factory;
		this.comparator = comparator;
		this.lanes.addAll(lanes);
	}

	public ShardComparator comparator() {
		return comparator;
	}

	public long comparedBytes() {
		return comparator.getComparedBytes();
	}

	public long mismatchCount() {
		return comparator.getMismatchCount();
	}

	public void stop() {
		if (!stopped.compareAndSet(false, true)) {
			return;
		}
		try {
			try {
				comparator.stop();
			} catch (Throwable t) {
				logger.error("[stop][comparator] cluster={} shard={}",
						comparator.getCluster(), comparator.getShard(), t);
			}
			List<CompareLane> toClose = new ArrayList<>(lanes);
			lanes.clear();
			for (CompareLane lane : toClose) {
				closeQuietly(lane);
			}
			try {
				factory.release(shardDbId);
			} catch (Throwable t) {
				logger.error("[stop][release] cluster={} shard={} dbId={}",
						comparator.getCluster(), comparator.getShard(), shardDbId, t);
			}
		} finally {
			group.shutdownGracefully(0, EVENT_LOOP_SHUTDOWN_SECONDS, TimeUnit.SECONDS)
					.awaitUninterruptibly(EVENT_LOOP_SHUTDOWN_SECONDS, TimeUnit.SECONDS);
		}
	}

	private void closeQuietly(CompareLane lane) {
		if (lane == null) {
			return;
		}
		try {
			factory.close(lane);
		} catch (Throwable t) {
			logger.error("[stop][close] cluster={} shard={} keeper={}",
					comparator.getCluster(), comparator.getShard(), lane.getAddress(), t);
		}
	}

	public String describe() {
		StringBuilder sb = new StringBuilder();
		sb.append("cluster=").append(comparator.getCluster());
		sb.append(" shard=").append(comparator.getShard());
		sb.append(" aligned=").append(comparator.isCompareOffsetAligned());
		sb.append(" comparedBytes=").append(comparator.getComparedBytes());
		sb.append(" mismatchCount=").append(comparator.getMismatchCount());
		sb.append(" replIdMismatchCount=").append(comparator.getReplIdMismatchCount());
		sb.append(" compareLost=").append(comparator.getCompareLostCount());
		for (CompareLane lane : comparator.getLanes()) {
			sb.append(" | ").append(describeLane(lane));
		}
		return sb.toString();
	}

	private static String describeLane(CompareLane lane) {
		String replId = lane.getReplId();
		StreamRingBuffer buffer = lane.getBuffer();
		StringBuilder sb = new StringBuilder();
		sb.append(lane.getAddress());
		sb.append(" replId=").append(replId);
		if (replId != null) {
			sb.append(" continue=").append(lane.getContinueOffset());
		}
		if (buffer != null) {
			sb.append(" receivedEnd=").append(buffer.getReceivedEnd());
			sb.append(" bufferStart=").append(buffer.getBufferStart());
		}
		sb.append(" reconnect=").append(lane.getStreamReconnectCount());
		return sb.toString();
	}
}

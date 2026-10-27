package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.redis.comparator.compare.CompareLane;
import com.ctrip.xpipe.redis.comparator.compare.ShardComparator;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.comparator.meta.KeeperStreamFactory;
import com.ctrip.xpipe.redis.comparator.report.CompareReporter;
import com.ctrip.xpipe.redis.comparator.stream.StreamRingBuffer;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Phase CH review：一路 {@code close} 失败不得跳过其余回收。
 */
public class TfsComparatorHarnessStopTest {

	private static final int GROUP_SHUTDOWN_SECONDS = 3;

	@Test
	public void testStopContinuesAfterLaneCloseThrows() throws Exception {
		NioEventLoopGroup group = new NioEventLoopGroup(1,
				XpipeThreadFactory.create("tfs-ch-stop-test", true));
		ShardComparator comparator = new ShardComparator("c", "s", Collections.emptyList(),
				new ComparatorConfig(), CompareReporter.NOOP);
		comparator.start();
		RecordingFactory factory = new RecordingFactory();
		List<CompareLane> lanes = Arrays.asList(new StubLane("a:1"), new StubLane("b:2"));
		TfsComparatorHarness harness = new TfsComparatorHarness(7L, group, factory, comparator, lanes);
		try {
			harness.stop();
			harness.stop();
			Assert.assertEquals(Arrays.asList("a:1", "b:2"), factory.closes);
			Assert.assertEquals(Collections.singletonList(7L), factory.releases);
			Assert.assertFalse(comparator.isRunning());
			Assert.assertTrue(group.isShuttingDown() || group.isShutdown());
		} finally {
			group.shutdownGracefully(0, GROUP_SHUTDOWN_SECONDS, TimeUnit.SECONDS)
					.awaitUninterruptibly(GROUP_SHUTDOWN_SECONDS, TimeUnit.SECONDS);
		}
	}

	private static final class RecordingFactory implements KeeperStreamFactory {

		private final List<String> closes = new ArrayList<>();

		private final List<Long> releases = new ArrayList<>();

		@Override
		public CompareLane open(long shardDbId, String cluster, String shard, Endpoint endpoint,
				Runnable dataAvailable) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void close(CompareLane lane) {
			closes.add(lane.getAddress());
			if ("a:1".equals(lane.getAddress())) {
				throw new IllegalStateException("close-fail");
			}
		}

		@Override
		public void release(long shardDbId) {
			releases.add(shardDbId);
		}
	}

	private static final class StubLane implements CompareLane {

		private final String address;

		private StubLane(String address) {
			this.address = address;
		}

		@Override
		public String getAddress() {
			return address;
		}

		@Override
		public String getReplId() {
			return null;
		}

		@Override
		public long getContinueOffset() {
			return 0;
		}

		@Override
		public StreamRingBuffer getBuffer() {
			return null;
		}

		@Override
		public void disconnect() {
		}

		@Override
		public void reconnect() {
		}

		@Override
		public void close() {
		}
	}
}

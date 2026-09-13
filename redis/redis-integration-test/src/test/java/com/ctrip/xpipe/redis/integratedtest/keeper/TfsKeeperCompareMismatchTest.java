package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.redis.comparator.report.CompareReporter;
import com.ctrip.xpipe.redis.comparator.report.CompareReporter.LaneBytes;
import com.ctrip.xpipe.redis.comparator.report.CompareReporter.MismatchReport;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;
import com.ctrip.xpipe.redis.keeper.store.AsyncCommandStore;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import io.netty.buffer.Unpooled;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 两套独立 TFS store 的真实尾部失配串测（spec D40 / AC-27）。
 */
public class TfsKeeperCompareMismatchTest extends AbstractTfsKeeperIntegrated {

	private static final int PAYLOAD_VALUE_BYTES = 512;

	@Override
	protected boolean useDivergentStoreDirs() {
		return true;
	}

	/**
	 * Active 先形成可用 store，再把完整 manager 目录复制给尚未进入 PREPARE 的另一路。
	 */
	@Override
	protected void makeTfsKeeperRight() throws Exception {
		RedisMeta master = getRedisMaster();
		KeeperMeta active = getKeeperActive();
		List<KeeperMeta> backups = getKeepersBackup();
		if (backups.isEmpty()) {
			throw new IllegalStateException("TFS mismatch topology needs Active + Prepare keeper");
		}
		KeeperMeta prepare = backups.get(0);

		setKeeperState(active, KeeperState.ACTIVE, master.getIp(), master.getPort());
		waitTfsActiveReady(active);
		DefaultReplicationStore activeStore = openedStore(active);
		long beforeSeed = activeStore.getCurReplStageReplOff();
		try (Jedis jedis = createJedis(master)) {
			jedis.set("tfs-mm-seed", randomString(PAYLOAD_VALUE_BYTES));
		}
		waitConditionUntilTimeOut(() -> activeStore.getCurReplStageReplOff() > beforeSeed,
				READY_WAIT_MILLI, READY_POLL_MILLI);
		flushAndFsync(activeStore, "seed before store copy");

		ReplicationStoreManager activeManager = tfsStoreManager(active);
		ReplicationStoreManager prepareManager = tfsStoreManager(prepare);
		File activeManagerDir = activeManager.getBaseDir();
		File prepareManagerDir = prepareManager.getBaseDir();
		if (activeManagerDir.getCanonicalFile().equals(prepareManagerDir.getCanonicalFile())) {
			throw new IllegalStateException("mismatch test requires two keeperBaseDir: " + activeManagerDir);
		}
		if (prepareManager.getLifecycleState().canStop()) {
			prepareManager.stop();
		}
		FileUtils.deleteDirectory(prepareManagerDir);
		FileUtils.copyDirectory(activeManagerDir, prepareManagerDir);

		setKeeperState(prepare, KeeperState.PREPARE, master.getIp(), master.getPort());
		waitTfsPrepareReady(prepare);
	}

	@Test
	public void testDifferentLatestCommandTailsReportMismatchAndContinue() throws Exception {
		Assert.assertNotEquals(tfsStoreManager(activeKeeper).getBaseDir().getCanonicalFile(),
				tfsStoreManager(backupKeeper).getBaseDir().getCanonicalFile());

		RecordingCompareReporter reporter = new RecordingCompareReporter();
		startComparator(reporter);
		waitAligned();

		DefaultRedisKeeperServer activeServer = tfsKeeperServer(activeKeeper);
		activeServer.stopAndDisposeMaster();
		Assert.assertNull("Active Redis master must be detached before manual tail writes",
				activeServer.getRedisMaster());

		DefaultReplicationStore activeStore = openedStore(activeKeeper);
		DefaultReplicationStore prepareStore = openedStore(backupKeeper);
		Assert.assertEquals(activeStore.getMetaStore().dupReplicationStoreMeta().getCmdFilePrefix(),
				prepareStore.getMetaStore().dupReplicationStoreMeta().getCmdFilePrefix());

		byte[] activeMismatch = respSet("tfs-mm-divergent", repeated('A', PAYLOAD_VALUE_BYTES));
		byte[] prepareMismatch = respSet("tfs-mm-divergent", repeated('B', PAYLOAD_VALUE_BYTES));
		Assert.assertEquals(activeMismatch.length, prepareMismatch.length);
		long mismatchBefore = comparatorHarness.mismatchCount();
		long activeEndBefore = activeStore.getCurReplStageReplOff();

		appendActive(activeStore, activeMismatch, "active mismatch tail");
		appendPrepare(activeStore, prepareStore, prepareMismatch, "prepare mismatch tail");
		waitMismatch(reporter, mismatchBefore);

		long activeEndAfter = activeStore.getCurReplStageReplOff();
		Assert.assertEquals(activeEndBefore + activeMismatch.length, activeEndAfter);
		MismatchReport report = reporter.firstMismatch();
		Assert.assertNotNull(report);
		Assert.assertTrue("report offset must point into the newly appended tail: "
				+ report.getMasterReplOffset(),
				report.getMasterReplOffset() >= activeEndBefore + 1
						&& report.getMasterReplOffset() <= activeEndAfter);
		assertKeeperAddresses(report);
		Assert.assertEquals(2, report.getLanes().size());
		Assert.assertFalse("reported lane chunks must contain the divergent bytes",
				Arrays.equals(report.getLanes().get(0).getChunk(), report.getLanes().get(1).getChunk()));
		// onMismatch 回调发生在 compareOnce 提交 comparedBytes 之前；先等失配块完整提交，
		// 再截取 continuation 基线，避免把失配块的迟到进度误当成后续一致数据。
		waitComparedBytesGrowBy(report.getComparedBytes(), report.getLanes().get(0).getChunk().length);

		long comparedAfterMismatch = comparatorHarness.comparedBytes();
		long mismatchAfterReport = comparatorHarness.mismatchCount();
		byte[] consistentTail = respSet("tfs-mm-continue", repeated('C', PAYLOAD_VALUE_BYTES));
		appendActive(activeStore, consistentTail, "active consistent tail");
		appendPrepare(activeStore, prepareStore, consistentTail, "prepare consistent tail");
		waitComparedBytesGrowBy(comparedAfterMismatch, consistentTail.length);

		Assert.assertTrue(comparatorHarness.comparedBytes()
				>= comparedAfterMismatch + consistentTail.length);
		Assert.assertEquals("equal bytes after the mismatch must not create another mismatch",
				mismatchAfterReport, comparatorHarness.mismatchCount());
	}

	private DefaultReplicationStore openedStore(KeeperMeta keeper) {
		ReplicationStore store = tfsStoreManager(keeper).getOpenedStore();
		if (!(store instanceof DefaultReplicationStore)) {
			throw new IllegalStateException("keeper has no opened DefaultReplicationStore: " + keeper);
		}
		return (DefaultReplicationStore) store;
	}

	private void appendActive(DefaultReplicationStore store, byte[] payload, String operation)
			throws Exception {
		Assert.assertEquals(payload.length, store.appendCommands(Unpooled.wrappedBuffer(payload)));
		flushAndFsync(store, operation);
	}

	private void appendPrepare(DefaultReplicationStore activeStore, DefaultReplicationStore prepareStore,
			byte[] payload, String operation) throws Exception {
		AsyncCommandStore activeCommandStore = asyncCommandStore(activeStore);
		AsyncFileSystem fs = activeCommandStore.getAsyncFileSystem();
		String prefix = prepareStore.getMetaStore().dupReplicationStoreMeta().getCmdFilePrefix();
		if (prefix == null) {
			throw new IllegalStateException("Prepare store has no command file prefix");
		}
		AsyncSegmentFile writer = AsyncFileSystemHelper.awaitOpen(fs,
				() -> fs.open(prepareStore.getBaseDir().getAbsolutePath(), prefix,
						Collections.emptyList(), true,
						activeCommandStore.getFileSystemReplId().toString()),
				"open " + operation);
		try {
			AsyncFileSystemHelper.writeAndAwait(fs, writer, Unpooled.wrappedBuffer(payload),
					payload.length, operation);
			AsyncFileSystemHelper.await(fs.fsync(writer), "fsync " + operation);
		} finally {
			AsyncFileSystemHelper.closeHandle(fs, writer, "close " + operation);
		}
	}

	private void flushAndFsync(DefaultReplicationStore store, String operation) throws Exception {
		store.flushPendingData();
		AsyncCommandStore commandStore = asyncCommandStore(store);
		AsyncFileSystemHelper.await(commandStore.getAsyncFileSystem().fsync(commandStore.getWriteSegmentFile()),
				"fsync " + operation);
	}

	private AsyncCommandStore asyncCommandStore(DefaultReplicationStore store) {
		if (!(store.getCommandStore() instanceof AsyncCommandStore)) {
			throw new IllegalStateException("store has no writable AsyncCommandStore: " + store);
		}
		return (AsyncCommandStore) store.getCommandStore();
	}

	private void waitMismatch(RecordingCompareReporter reporter, long mismatchBefore) throws Exception {
		try {
			waitConditionUntilTimeOut(() -> reporter.firstMismatch() != null
						&& comparatorHarness.mismatchCount() > mismatchBefore,
					READY_WAIT_MILLI, READY_POLL_MILLI);
		} catch (TimeoutException e) {
			TimeoutException dump = new TimeoutException(describeComparatorWait(
					"waitMismatch from=" + mismatchBefore));
			dump.initCause(e);
			throw dump;
		}
	}

	private void waitComparedBytesGrowBy(long from, long minimumBytes) throws Exception {
		try {
			waitConditionUntilTimeOut(() -> comparatorHarness.comparedBytes() >= from + minimumBytes,
					READY_WAIT_MILLI, READY_POLL_MILLI);
		} catch (TimeoutException e) {
			TimeoutException dump = new TimeoutException(describeComparatorWait(
					"waitComparedBytesGrowBy from=" + from + " minimumBytes=" + minimumBytes));
			dump.initCause(e);
			throw dump;
		}
	}

	private void assertKeeperAddresses(MismatchReport report) {
		Set<String> actual = new HashSet<>();
		for (LaneBytes lane : report.getLanes()) {
			actual.add(lane.getAddress());
		}
		Set<String> expected = new HashSet<>(Arrays.asList(
				keeperAddress(activeKeeper), keeperAddress(backupKeeper)));
		Assert.assertEquals(expected, actual);
	}

	private String keeperAddress(KeeperMeta keeper) {
		Endpoint endpoint = keeperEndpoint(keeper);
		return endpoint.getHost() + ":" + endpoint.getPort();
	}

	private static byte[] respSet(String key, String value) {
		byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
		byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
		String command = "*3\r\n$3\r\nSET\r\n$" + keyBytes.length + "\r\n" + key
				+ "\r\n$" + valueBytes.length + "\r\n" + value + "\r\n";
		return command.getBytes(StandardCharsets.UTF_8);
	}

	private static String repeated(char value, int count) {
		char[] chars = new char[count];
		Arrays.fill(chars, value);
		return new String(chars);
	}

	private static final class RecordingCompareReporter implements CompareReporter {
		private final AtomicReference<MismatchReport> firstMismatch = new AtomicReference<>();

		@Override
		public void onMismatch(MismatchReport report) {
			firstMismatch.compareAndSet(null, report);
		}

		@Override
		public void onCompareLost(String cluster, String shard, long comparedEndBefore,
				long comparedEndAfter) {
		}

		@Override
		public void onReplIdMismatch(String cluster, String shard, List<String> replIds) {
		}

		MismatchReport firstMismatch() {
			return firstMismatch.get();
		}
	}
}

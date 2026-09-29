package com.ctrip.xpipe.redis.keeper.prepare;

import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import com.ctrip.xpipe.redis.keeper.store.readonly.ReadOnlyCommandStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Phase OS (T-OS.5 ①②③⑨)：Watcher 侧的 30s 二分。AC-6b ②③④⑥。
 * <p>
 * 用 mock FS + mock Store 直接构造 FS-M5.6 的「新段可见、老段 size 滞后」，并用注入时钟推进宽限，
 * 不睡墙钟。两相节奏本身的断言在 {@link PrepareStoreWatcherPhaseTest}。
 */
@RunWith(MockitoJUnitRunner.class)
public class PrepareStoreWatcherLocateMissTest {

	private static final File CMD_FILE = new File("/data/repl_1/store_abc/cmd_");

	private static final String STORE_DIR_NAME = "store_abc";

	private static final int REOPEN_INTERVAL_MILLI = 5000;

	private static final int CLOSE_HOLD_MILLI = 1000;

	@Mock
	private AsyncFileSystem asyncFileSystem;

	@Mock
	private AsyncSegmentFile handle;

	@Mock
	private ReplicationStoreManager manager;

	@Mock
	private DefaultReplicationStore replicationStore;

	private ReadOnlyCommandStore cmdStore;

	private PrepareStoreWatcher watcher;

	private final AtomicLong now = new AtomicLong();

	private final List<String> changes = new ArrayList<>();

	@Before
	public void setUp() throws Exception {
		now.set(0);
		changes.clear();
		Mockito.lenient().when(asyncFileSystem.open(Mockito.anyString(), Mockito.anyString(), Mockito.anyList(),
						Mockito.eq(false), Mockito.anyString()))
				.thenReturn(CompletableFuture.completedFuture(handle));
		Mockito.lenient().when(asyncFileSystem.close(handle))
				.thenReturn(CompletableFuture.completedFuture(null));

		cmdStore = new ReadOnlyCommandStore(CMD_FILE, asyncFileSystem, ReplId.from(1L));
		stubChain(new long[]{0L}, 100L);
		cmdStore.initialize();

		Mockito.lenient().when(replicationStore.getCommandStore()).thenReturn(cmdStore);
		Mockito.lenient().when(replicationStore.getBaseDir()).thenReturn(new File("/data/repl_1/" + STORE_DIR_NAME));
		Mockito.lenient().when(replicationStore.backlogEndOffset()).thenAnswer(in -> cmdStore.totalLength());
		Mockito.lenient().when(replicationStore.backlogBeginOffset()).thenAnswer(in -> cmdStore.lowestAvailableOffset());
		Mockito.lenient().when(manager.getOpenedStore()).thenReturn(replicationStore);
		Mockito.lenient().when(manager.reloadLatestStoreDir()).thenReturn(STORE_DIR_NAME);

		TestKeeperConfig keeperConfig = new TestKeeperConfig();
		keeperConfig.setPrepareWatchReopenIntervalMilli(REOPEN_INTERVAL_MILLI);
		keeperConfig.setPrepareWatchCloseHoldMilli(CLOSE_HOLD_MILLI);
		watcher = new PrepareStoreWatcher(manager, keeperConfig, changes::add);
		watcher.setClock(now::get);
	}

	/**
	 * AC-6b ②：段不连续时 Watcher 关掉句柄退回关相、快照整份保留、**未**断 slave / 未 release；
	 * AC-6b ③：老段补齐后下一个开相恢复并推进，计时清零。
	 */
	@Test
	public void testLocateMissWithinGraceClosesHandlesAndRetries() throws Exception {
		driveToOpenPhaseAction();
		Assert.assertEquals(PrepareStoreWatcher.Phase.OPEN, watcher.getPhase());
		Assert.assertEquals(100L, cmdStore.totalLength());

		// 老段掉链：只剩新段 [200, 250)
		stubChain(new long[]{200L}, 50L);
		driveToOpenPhaseAction();

		Assert.assertEquals(PrepareStoreWatcher.Phase.CLOSED, watcher.getPhase());
		Assert.assertFalse("locate miss must close the cmd handle", cmdStore.isHandleOpen());
		Assert.assertEquals(100L, cmdStore.totalLength());
		Assert.assertEquals(0L, cmdStore.lowestAvailableOffset());
		Assert.assertEquals(now.get(), watcher.getLocateMissSinceMillis());
		Assert.assertTrue(changes.isEmpty());
		Assert.assertEquals(0, watcher.getStoreSwitchedCount());
		Assert.assertEquals(0, watcher.getChainRebuiltCount());
		Mockito.verify(manager, Mockito.never()).releaseCurrentStore();

		// 老段 size 补齐 ⇒ 下一个开相恢复 OPENED、计时清零
		stubChain(new long[]{0L, 200L}, 50L);
		now.addAndGet(CLOSE_HOLD_MILLI);
		watcher.pollOnce();

		Assert.assertEquals(PrepareStoreWatcher.Phase.OPEN, watcher.getPhase());
		Assert.assertTrue(cmdStore.isHandleOpen());
		Assert.assertEquals(250L, cmdStore.totalLength());
		Assert.assertEquals(-1L, watcher.getLocateMissSinceMillis());
		Assert.assertEquals(0, watcher.getChainRebuiltCount());
		Assert.assertNotNull(watcher.getSnapshot());
		Assert.assertEquals(250L, watcher.getSnapshot().getTotalLength());
	}

	/**
	 * AC-6b ④：满 {@code REOPEN_LOCATE_GRACE_MILLI} 转「cmd 非法」—— 快照按新链条重建、ERROR 日志，
	 * 且**句柄仍打开、Store 未被 release、未 onStoreChanged**。
	 */
	@Test
	public void testLocateMissBeyondGraceRebuildsSnapshotAndKeepsStore() throws Exception {
		driveToOpenPhaseAction();
		Assert.assertEquals(100L, cmdStore.totalLength());

		// 占槽方原地重置：段起点不变、长度回退 —— 重建必须让 observedEnd 真的回退
		stubChain(new long[]{0L}, 30L);
		driveToOpenPhaseAction();
		long missSince = watcher.getLocateMissSinceMillis();
		Assert.assertEquals(now.get(), missSince);
		Assert.assertEquals(100L, cmdStore.totalLength());

		// 宽限内重试若干轮：始终保留旧快照，计时不刷新
		for (int i = 0; i < 3; i++) {
			now.addAndGet(CLOSE_HOLD_MILLI);
			watcher.pollOnce();
			Assert.assertEquals(PrepareStoreWatcher.Phase.CLOSED, watcher.getPhase());
			Assert.assertEquals(missSince, watcher.getLocateMissSinceMillis());
			Assert.assertEquals(100L, cmdStore.totalLength());
			Assert.assertEquals(0, watcher.getChainRebuiltCount());
		}

		// 宽限用尽
		now.set(missSince + PrepareStoreWatcher.REOPEN_LOCATE_GRACE_MILLI);
		watcher.pollOnce();

		Assert.assertEquals(PrepareStoreWatcher.Phase.OPEN, watcher.getPhase());
		Assert.assertEquals(1, watcher.getChainRebuiltCount());
		Assert.assertEquals("observedEnd must be allowed to regress", 30L, cmdStore.totalLength());
		Assert.assertTrue("handle must stay open after rebuild", cmdStore.isHandleOpen());
		Assert.assertEquals(-1L, watcher.getLocateMissSinceMillis());
		Assert.assertTrue("rebuild must not disconnect slaves", changes.isEmpty());
		Assert.assertEquals(0, watcher.getStoreSwitchedCount());
		Mockito.verify(manager, Mockito.never()).releaseCurrentStore();
		Assert.assertNotNull(watcher.getSnapshot());
		Assert.assertEquals(30L, watcher.getSnapshot().getTotalLength());
	}

	/**
	 * AC-6b ⑥：{@code FAILED}（open / list / size 抛 IO 异常）只关句柄重试，**不**计入 30s 宽限、
	 * **不**重建快照。
	 */
	@Test
	public void testObserveFailureDoesNotConsumeLocateGrace() throws Exception {
		driveToOpenPhaseAction();
		Assert.assertEquals(100L, cmdStore.totalLength());

		Mockito.when(asyncFileSystem.sizeOfSegment(handle, 0L))
				.thenReturn(CompletableFuture.failedFuture(new RuntimeException("size fail")));
		for (int i = 0; i < 3; i++) {
			driveToOpenPhaseAction();
			Assert.assertEquals(PrepareStoreWatcher.Phase.CLOSED, watcher.getPhase());
			Assert.assertEquals("FAILED must not start the locate grace timer",
					-1L, watcher.getLocateMissSinceMillis());
			Assert.assertEquals(0, watcher.getChainRebuiltCount());
			Assert.assertEquals(100L, cmdStore.totalLength());
		}

		// FS 恢复后正常回开相
		stubChain(new long[]{0L}, 120L);
		now.addAndGet(CLOSE_HOLD_MILLI);
		watcher.pollOnce();
		Assert.assertEquals(PrepareStoreWatcher.Phase.OPEN, watcher.getPhase());
		Assert.assertEquals(120L, cmdStore.totalLength());
	}

	/**
	 * 把一个完整 cycle 驱到「开相动作」那一 poll（开相到点 → 关相静置到点 → open + 观察）。
	 */
	private void driveToOpenPhaseAction() throws Exception {
		if (watcher.getPhase() == PrepareStoreWatcher.Phase.OPEN) {
			watcher.pollOnce();
			now.addAndGet(REOPEN_INTERVAL_MILLI);
			watcher.pollOnce();
		}
		now.addAndGet(CLOSE_HOLD_MILLI);
		watcher.pollOnce();
	}

	private void stubChain(long[] segmentStarts, long lastSegmentSize) {
		List<Long> starts = new ArrayList<>();
		for (long start : segmentStarts) {
			starts.add(start);
		}
		Mockito.when(asyncFileSystem.list(handle)).thenReturn(starts);
		Mockito.when(asyncFileSystem.sizeOfSegment(handle, segmentStarts[segmentStarts.length - 1]))
				.thenReturn(CompletableFuture.completedFuture(lastSegmentSize));
	}
}

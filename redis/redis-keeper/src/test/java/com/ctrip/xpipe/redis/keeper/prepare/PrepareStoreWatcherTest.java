package com.ctrip.xpipe.redis.keeper.prepare;

import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.storage.AbstractStorageFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.store.readonly.ReadOnlyCommandStore;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Phase WT (T-WT.4) / Phase HW (T-HW.6): PrepareStoreWatcher 的变更感知语义。AC-5 / D8 / D11 / D48。
 * <p>
 * 返工后所有「感知 + reload + 观察」动作都发生在**关相到点后的那一次开相** poll 上，因此用注入时钟把
 * 一个完整 cycle 驱完（{@link #driveOpenPhaseAction}），不再靠单次 {@code pollOnce()}。
 * 两相节奏本身的断言在 {@link PrepareStoreWatcherPhaseTest}。
 */
public class PrepareStoreWatcherTest extends AbstractRedisKeeperTest {

	private static final String REPL_ID = "000000000000000000000000000000000000000A";

	private static final String REPL_ID_B = "000000000000000000000000000000000000000B";

	private static final int REOPEN_INTERVAL_MILLI = 5000;

	private static final int CLOSE_HOLD_MILLI = 1000;

	private TestKeeperConfig keeperConfig;

	private final AtomicLong now = new AtomicLong();

	@Before
	public void beforePrepareStoreWatcherTest() {
		keeperConfig = new TestKeeperConfig();
		keeperConfig.setReplicationStoreGcIntervalSeconds(60);
		keeperConfig.setMinTimeMilliToGcAfterCreate(60_000);
		keeperConfig.setPrepareWatchReopenIntervalMilli(REOPEN_INTERVAL_MILLI);
		keeperConfig.setPrepareWatchCloseHoldMilli(CLOSE_HOLD_MILLI);
		now.set(0);
	}

	@Test
	public void testStoreDirChangeReleasesAndDisconnectsWithinOneOpenPhase() throws Exception {
		AsyncFileSystem fs = createTestAsyncFileSystem();
		File base = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultReplicationStoreManager occupying = newManager(fs, base, runid);
		DefaultReplicationStoreManager watching = newManager(fs, base, runid);
		List<String> changes = new ArrayList<>();
		AtomicInteger slavesClosed = new AtomicInteger();
		PrepareStoreWatcher watcher = newWatcher(watching, reason -> {
			changes.add(reason);
			slavesClosed.incrementAndGet();
		});
		try {
			LifecycleHelper.initializeIfPossible(occupying);
			LifecycleHelper.startIfPossible(occupying);
			DefaultReplicationStore storeA = (DefaultReplicationStore) occupying.create();
			seedCommands(storeA);
			File dirA = storeA.getBaseDir();

			LifecycleHelper.initializeIfPossible(watching);
			watching.setReadOnly(true);
			LifecycleHelper.startIfPossible(watching);
			Assert.assertEquals(dirA, ((DefaultReplicationStore) watching.getCurrent()).getBaseDir());
			driveOpenPhaseAction(watcher);
			Assert.assertEquals(dirA, ((DefaultReplicationStore) watching.getOpenedStore()).getBaseDir());
			Assert.assertEquals(0, watcher.getStoreSwitchedCount());

			DefaultReplicationStore storeB = (DefaultReplicationStore) occupying.create();
			seedCommands(storeB);
			File dirB = storeB.getBaseDir();
			Assert.assertNotEquals(dirA, dirB);

			driveOpenPhaseAction(watcher);

			Assert.assertNull(watching.getOpenedStore());
			Assert.assertNull(watcher.getSnapshot());
			Assert.assertEquals(1, changes.size());
			Assert.assertEquals("latest.store.dir", changes.get(0));
			Assert.assertEquals(1, slavesClosed.get());
			Assert.assertEquals(1, watcher.getStoreSwitchedCount());

			Assert.assertEquals(dirB, ((DefaultReplicationStore) watching.getCurrent()).getBaseDir());
			Assert.assertEquals(dirB, ((DefaultReplicationStore) watching.getOpenedStore()).getBaseDir());
		} finally {
			watcher.stop();
			stopDispose(watching);
			stopDispose(occupying);
			fs.shutdown();
		}
	}

	/**
	 * Full sync on the holder: create() publishes the new store B at once, the prefix only lands at confirmRdb after the
	 * RDB transfer. Old store A is released; during the window consumers get no store (never a cmdStore-less one);
	 * once B has its chain the next open reads B.
	 */
	@Test
	public void testFullSyncStoreSwitchWaitsForNewChain() throws Exception {
		AsyncFileSystem fs = createTestAsyncFileSystem();
		File base = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultReplicationStoreManager occupying = newManager(fs, base, runid);
		DefaultReplicationStoreManager watching = newManager(fs, base, runid);
		List<String> changes = new ArrayList<>();
		PrepareStoreWatcher watcher = newWatcher(watching, changes::add);
		try {
			LifecycleHelper.initializeIfPossible(occupying);
			LifecycleHelper.startIfPossible(occupying);
			DefaultReplicationStore storeA = (DefaultReplicationStore) occupying.create();
			seedCommands(storeA);

			LifecycleHelper.initializeIfPossible(watching);
			watching.setReadOnly(true);
			LifecycleHelper.startIfPossible(watching);
			Assert.assertEquals(storeA.getBaseDir(), ((DefaultReplicationStore) watching.getCurrent()).getBaseDir());
			driveOpenPhaseAction(watcher);

			// full sync starts: new store published, no cmd chain until the RDB is confirmed
			DefaultReplicationStore storeB = (DefaultReplicationStore) occupying.create();
			driveOpenPhaseAction(watcher);

			Assert.assertEquals(Arrays.asList("latest.store.dir"), changes);
			Assert.assertNull("old store released", watching.getOpenedStore());
			Assert.assertNull("window: no readable store yet", watching.getCurrent());
			Assert.assertNull(watching.getOpenedStore());

			// RDB confirmed: chain appears in B
			seedCommands(storeB);
			DefaultReplicationStore opened = (DefaultReplicationStore) watching.getCurrent();
			Assert.assertEquals(storeB.getBaseDir(), opened.getBaseDir());
			Assert.assertTrue(opened.getCommandStore() instanceof ReadOnlyCommandStore);
			Assert.assertEquals(storeB.getMetaStore().dupReplicationStoreMeta().getCmdFilePrefix(),
					((ReadOnlyCommandStore) opened.getCommandStore()).getCommandFileNamePrefix());

			driveOpenPhaseAction(watcher);
			Assert.assertSame(opened, watching.getOpenedStore());
			Assert.assertEquals(1, watcher.getStoreSwitchedCount());
		} finally {
			watcher.stop();
			stopDispose(watching);
			stopDispose(occupying);
			fs.shutdown();
		}
	}

	/**
	 * Defensive: the cmd chain is replaced inside the same store dir (writer convention says it never is). The
	 * read-only chain is anchored to the old prefix, so the watcher must release the store like a store switch.
	 */
	@Test
	public void testSameDirCmdChainChangeReleasesStore() throws Exception {
		AsyncFileSystem fs = createTestAsyncFileSystem();
		File base = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultReplicationStoreManager occupying = newManager(fs, base, runid);
		DefaultReplicationStoreManager watching = newManager(fs, base, runid);
		List<String> changes = new ArrayList<>();
		PrepareStoreWatcher watcher = newWatcher(watching, changes::add);
		try {
			LifecycleHelper.initializeIfPossible(occupying);
			LifecycleHelper.startIfPossible(occupying);
			DefaultReplicationStore writable = (DefaultReplicationStore) occupying.create();
			seedCommands(writable);
			String oldPrefix = writable.getMetaStore().dupReplicationStoreMeta().getCmdFilePrefix();

			LifecycleHelper.initializeIfPossible(watching);
			watching.setReadOnly(true);
			LifecycleHelper.startIfPossible(watching);
			DefaultReplicationStore readOnly = (DefaultReplicationStore) watching.getCurrent();
			Assert.assertEquals(oldPrefix, ((ReadOnlyCommandStore) readOnly.getCommandStore()).getCommandFileNamePrefix());
			driveOpenPhaseAction(watcher);
			Assert.assertSame(readOnly, watching.getOpenedStore());

			// new chain in the same dir
			seedCommands(writable);
			String newPrefix = writable.getMetaStore().dupReplicationStoreMeta().getCmdFilePrefix();
			Assert.assertNotEquals(oldPrefix, newPrefix);
			Assert.assertEquals(readOnly.getBaseDir(), writable.getBaseDir());

			driveOpenPhaseAction(watcher);

			Assert.assertEquals(Arrays.asList("cmdFilePrefix"), changes);
			Assert.assertNull("stale-chain store released", watching.getOpenedStore());
			Assert.assertNull(watcher.getSnapshot());
			Assert.assertEquals(1, watcher.getStoreSwitchedCount());

			DefaultReplicationStore reopened = (DefaultReplicationStore) watching.getCurrent();
			Assert.assertEquals(newPrefix, ((ReadOnlyCommandStore) reopened.getCommandStore()).getCommandFileNamePrefix());
		} finally {
			watcher.stop();
			stopDispose(watching);
			stopDispose(occupying);
			fs.shutdown();
		}
	}

	@Test
	public void testDoesNotOpenWhenNotYetOpened() throws Exception {
		AsyncFileSystem fs = createTestAsyncFileSystem();
		File base = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultReplicationStoreManager occupying = newManager(fs, base, runid);
		DefaultReplicationStoreManager watching = newManager(fs, base, runid);
		List<String> changes = new ArrayList<>();
		PrepareStoreWatcher watcher = newWatcher(watching, changes::add);
		try {
			LifecycleHelper.initializeIfPossible(occupying);
			LifecycleHelper.startIfPossible(occupying);
			DefaultReplicationStore storeA = (DefaultReplicationStore) occupying.create();
			seedCommands(storeA);
			File dirA = storeA.getBaseDir();

			LifecycleHelper.initializeIfPossible(watching);
			watching.setReadOnly(true);
			LifecycleHelper.startIfPossible(watching);
			Assert.assertNull(watching.getOpenedStore());

			driveOpenPhaseAction(watcher);

			Assert.assertNull(watching.getOpenedStore());
			Assert.assertNull(watcher.getSnapshot());
			Assert.assertEquals(0, changes.size());
			Assert.assertEquals(0, watcher.getStoreSwitchedCount());

			Assert.assertEquals(dirA, ((DefaultReplicationStore) watching.getCurrent()).getBaseDir());
		} finally {
			watcher.stop();
			stopDispose(watching);
			stopDispose(occupying);
			fs.shutdown();
		}
	}

	@Test
	public void testSameDirReplIdChangeDoesNotRebuild() throws Exception {
		AsyncFileSystem fs = createTestAsyncFileSystem();
		File base = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultReplicationStoreManager occupying = newManager(fs, base, runid);
		DefaultReplicationStoreManager watching = newManager(fs, base, runid);
		List<String> changes = new ArrayList<>();
		PrepareStoreWatcher watcher = newWatcher(watching, changes::add);
		try {
			LifecycleHelper.initializeIfPossible(occupying);
			LifecycleHelper.startIfPossible(occupying);
			DefaultReplicationStore writable = (DefaultReplicationStore) occupying.create();
			seedCommands(writable);
			File dir = writable.getBaseDir();

			LifecycleHelper.initializeIfPossible(watching);
			watching.setReadOnly(true);
			LifecycleHelper.startIfPossible(watching);
			Assert.assertEquals(dir, ((DefaultReplicationStore) watching.getCurrent()).getBaseDir());
			driveOpenPhaseAction(watcher);
			ReplicationStore opened = watching.getOpenedStore();
			Assert.assertEquals(dir, ((DefaultReplicationStore) opened).getBaseDir());
			Assert.assertEquals(REPL_ID, opened.getMetaStore().getCurrentReplStage().getReplId());

			writable.psyncContinue(REPL_ID_B);
			Assert.assertEquals(REPL_ID, opened.getMetaStore().getCurrentReplStage().getReplId());
			driveOpenPhaseAction(watcher);

			ReplicationStore still = watching.getOpenedStore();
			Assert.assertSame(opened, still);
			Assert.assertEquals(dir, ((DefaultReplicationStore) still).getBaseDir());
			Assert.assertEquals(REPL_ID_B, still.getMetaStore().getCurrentReplStage().getReplId());
			Assert.assertEquals(0, changes.size());
			Assert.assertEquals(0, watcher.getStoreSwitchedCount());
		} finally {
			watcher.stop();
			stopDispose(watching);
			stopDispose(occupying);
			fs.shutdown();
		}
	}

	/**
	 * doc/keeper-tfs-bugfix-prefix-null.md: while the holder has not written cmdFilePrefix, a read-only open keeps
	 * no store (it could never get a cmdStore). After the prefix lands, the consumer's next open gets a readable store.
	 */
	@Test
	public void testNoStoreWhileFreshThenOpensAfterChain() throws Exception {
		AsyncFileSystem fs = createTestAsyncFileSystem();
		File base = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultReplicationStoreManager occupying = newManager(fs, base, runid);
		DefaultReplicationStoreManager watching = newManager(fs, base, runid);
		PrepareStoreWatcher watcher = newWatcher(watching, PrepareStoreChangeListener.NOOP);
		try {
			LifecycleHelper.initializeIfPossible(occupying);
			LifecycleHelper.startIfPossible(occupying);
			DefaultReplicationStore writable = (DefaultReplicationStore) occupying.create();

			LifecycleHelper.initializeIfPossible(watching);
			watching.setReadOnly(true);
			LifecycleHelper.startIfPossible(watching);
			Assert.assertNull("no cmd chain yet: nothing readable", watching.getCurrent());
			Assert.assertNull(watching.getOpenedStore());

			driveOpenPhaseAction(watcher);
			Assert.assertNull("watcher never opens a store by itself", watching.getOpenedStore());

			writable.psyncContinueFrom(REPL_ID, 1);

			// consumer (slave PSYNC / PrepareCmdParser) retries and now gets a store with a read-only cmd chain
			DefaultReplicationStore opened = (DefaultReplicationStore) watching.getCurrent();
			Assert.assertNotNull(opened);
			Assert.assertTrue(opened.getCommandStore() instanceof com.ctrip.xpipe.redis.keeper.store.readonly.ReadOnlyCommandStore);
			Assert.assertEquals(REPL_ID, opened.getMetaStore().getCurrentReplStage().getReplId());

			driveOpenPhaseAction(watcher);
			Assert.assertSame(opened, watching.getOpenedStore());
			Assert.assertEquals(0, watcher.getStoreSwitchedCount());
		} finally {
			watcher.stop();
			stopDispose(watching);
			stopDispose(occupying);
			fs.shutdown();
		}
	}

	/**
	 * AC-5b ⑦：tick 抛异常后下一 tick 仍执行。走真实 scheduler，tick 固定 1s，两个阈值压到最小，
	 * 因此首个快照大约在第 3 个 tick 出现。
	 */
	@Test
	public void testPollExceptionDoesNotStopNextCycle() throws Exception {
		keeperConfig.setPrepareWatchReopenIntervalMilli(1);
		keeperConfig.setPrepareWatchCloseHoldMilli(1);
		AsyncFileSystem fs = createTestAsyncFileSystem();
		File base = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultReplicationStoreManager occupying = newManager(fs, base, runid);
		DefaultReplicationStoreManager watching = newManager(fs, base, runid);
		PrepareStoreWatcher watcher = new PrepareStoreWatcher(watching, keeperConfig,
				PrepareStoreChangeListener.NOOP);
		try {
			LifecycleHelper.initializeIfPossible(occupying);
			LifecycleHelper.startIfPossible(occupying);
			seedCommands((DefaultReplicationStore) occupying.create());

			LifecycleHelper.initializeIfPossible(watching);
			watching.setReadOnly(true);
			LifecycleHelper.startIfPossible(watching);
			watching.getCurrent();
			watcher.failNextPoll(new RuntimeException("injected watch fail"));
			watcher.start();
			waitConditionUntilTimeOut(() -> watcher.getSnapshot() != null,
					6 * PrepareStoreWatcher.WATCH_TICK_MILLI);
			Assert.assertTrue(watcher.getPollCount() >= 2);
			Assert.assertNotNull(watcher.getSnapshot());
			Assert.assertEquals(0, watcher.getStoreSwitchedCount());
		} finally {
			watcher.stop();
			stopDispose(watching);
			stopDispose(occupying);
			fs.shutdown();
		}
	}

	@Test
	public void testSnapshotReadableWithoutFsAfterRefresh() throws Exception {
		AsyncFileSystem fs = spy(createTestAsyncFileSystem());
		File base = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultReplicationStoreManager occupying = newManager(fs, base, runid);
		DefaultReplicationStoreManager watching = newManager(fs, base, runid);
		PrepareStoreWatcher watcher = newWatcher(watching, PrepareStoreChangeListener.NOOP);
		try {
			LifecycleHelper.initializeIfPossible(occupying);
			LifecycleHelper.startIfPossible(occupying);
			seedCommands((DefaultReplicationStore) occupying.create());

			LifecycleHelper.initializeIfPossible(watching);
			watching.setReadOnly(true);
			LifecycleHelper.startIfPossible(watching);
			watching.getCurrent();
			driveOpenPhaseAction(watcher);

			PrepareWatchSnapshot snap = watcher.getSnapshot();
			Assert.assertNotNull(snap);
			clearInvocations(fs);

			long total = snap.getTotalLength();
			long backlog = snap.getBacklogEndOffset();
			Assert.assertEquals(total, backlog);
			Assert.assertTrue(total > 0);
			Assert.assertEquals(REPL_ID, snap.getMasterReplId());
			Assert.assertTrue(snap.getBacklogFirstByteOffset() >= 0);

			verify(fs, never()).open(anyString(), any(), any(AbstractStorageFile.ReplaceMode.class), anyBoolean(), any());
			verify(fs, never()).open(anyString(), anyString(), any(), anyBoolean(), anyString());
			verify(fs, never()).list(anyString());
			verify(fs, never()).list(any(AsyncSegmentFile.class));
			verify(fs, never()).size(any(AsyncFile.class));
			verify(fs, never()).size(any(AsyncSegmentFile.class));
			verify(fs, never()).sizeOfSegment(any(AsyncSegmentFile.class), anyLong());
			verify(fs, never()).read(any(AsyncFile.class), anyLong(), anyLong());
			verify(fs, never()).read(any(AsyncSegmentFile.class), anyLong(), anyLong());
		} finally {
			watcher.stop();
			stopDispose(watching);
			stopDispose(occupying);
			fs.shutdown();
		}
	}

	private PrepareStoreWatcher newWatcher(DefaultReplicationStoreManager manager,
										   PrepareStoreChangeListener listener) {
		PrepareStoreWatcher watcher = new PrepareStoreWatcher(manager, keeperConfig, listener);
		watcher.setClock(now::get);
		return watcher;
	}

	/**
	 * 把一个完整 cycle 驱到「开相动作」那一 poll：第一轮可能只做重绑，随后开相到点进关相，静置到点做
	 * 换店判定 / meta reload / {@code openAndObserve()}。全程用注入时钟，不睡墙钟。
	 */
	private void driveOpenPhaseAction(PrepareStoreWatcher watcher) throws IOException {
		watcher.pollOnce();
		now.addAndGet(REOPEN_INTERVAL_MILLI);
		watcher.pollOnce();
		now.addAndGet(CLOSE_HOLD_MILLI);
		watcher.pollOnce();
	}

	private DefaultReplicationStoreManager newManager(AsyncFileSystem fs, File base, String runid) {
		return new DefaultReplicationStoreManager(
				keeperConfig, getReplId(), runid, base,
				createkeeperMonitor(), mock(SyncRateManager.class), createRedisOpParser(), null, fs);
	}

	private void seedCommands(DefaultReplicationStore store) throws Exception {
		store.psyncContinueFrom(REPL_ID, 1);
		store.appendCommands(Unpooled.wrappedBuffer("*1\r\n$4\r\nPING\r\n".getBytes()));
	}

	private static void stopDispose(DefaultReplicationStoreManager manager) {
		try {
			LifecycleHelper.stopIfPossible(manager);
		} catch (Throwable ignore) {
		}
		try {
			LifecycleHelper.disposeIfPossible(manager);
		} catch (Throwable ignore) {
		}
	}
}

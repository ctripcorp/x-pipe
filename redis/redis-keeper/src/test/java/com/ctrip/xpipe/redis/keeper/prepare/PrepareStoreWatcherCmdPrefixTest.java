package com.ctrip.xpipe.redis.keeper.prepare;

import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.store.readonly.ReadOnlyCommandStore;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/** keeper-tfs-m6 T-P12.5 (watcher) */
public class PrepareStoreWatcherCmdPrefixTest extends AbstractRedisKeeperTest {

	private static final String REPL_ID = "000000000000000000000000000000000000000A";

	private static final byte[] PING = "*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII);

	private static final int REOPEN_INTERVAL_MILLI = 5000;

	private static final int CLOSE_HOLD_MILLI = 1000;

	private static final long BLOCKED_STOP_CHECK_MILLI = 300;

	private static final long FUTURE_WAIT_SECONDS = 10;

	private final AtomicLong now = new AtomicLong();

	private final List<String> changes = new CopyOnWriteArrayList<>();

	private TestKeeperConfig keeperConfig;

	private AsyncFileSystem fs;

	private DefaultReplicationStoreManager occupying;

	private DefaultReplicationStoreManager watching;

	private PrepareStoreWatcher watcher;

	@Before
	public void beforePrepareStoreWatcherCmdPrefixTest() {
		keeperConfig = new TestKeeperConfig();
		keeperConfig.setReplicationStoreGcIntervalSeconds(60);
		keeperConfig.setMinTimeMilliToGcAfterCreate(60_000);
		keeperConfig.setPrepareWatchReopenIntervalMilli(REOPEN_INTERVAL_MILLI);
		keeperConfig.setPrepareWatchCloseHoldMilli(CLOSE_HOLD_MILLI);
		now.set(0);
		fs = spy(createTestAsyncFileSystem());
		File base = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		occupying = newManager(base, runid);
		watching = spy(newManager(base, runid));
	}

	@After
	public void afterPrepareStoreWatcherCmdPrefixTest() {
		if (watcher != null) {
			watcher.stop();
		}
		stopDispose(watching);
		stopDispose(occupying);
		fs.shutdown();
	}

	@Test
	public void testFreshStoreBuildsCmdStoreInPlaceWithoutDisconnecting() throws Exception {
		DefaultReplicationStore writable = createOccupied();
		DefaultReplicationStore opened = openReadOnly();
		newWatcher();
		Assert.assertNull(opened.getCommandStore());

		driveOpenPhaseAction();
		Assert.assertNull(opened.getCommandStore());
		Assert.assertTrue(opened.isFresh());
		Assert.assertEquals(0L, watcher.getSnapshot().getTotalLength());

		writable.psyncContinueFrom(REPL_ID, 1);
		writable.appendCommands(Unpooled.wrappedBuffer(PING));
		driveOpenPhaseAction();

		Assert.assertSame("same store instance, no switch", opened, watching.getOpenedStore());
		Assert.assertTrue(opened.getCommandStore() instanceof ReadOnlyCommandStore);
		Assert.assertFalse(opened.isFresh());
		Assert.assertEquals(PrepareStoreWatcher.Phase.OPEN, watcher.getPhase());
		Assert.assertTrue("listener must not be called (no closeSlaves)", changes.isEmpty());
		Assert.assertEquals(0, watcher.getStoreSwitchedCount());
		long firstTotal = watcher.getSnapshot().getTotalLength();
		Assert.assertEquals(PING.length, firstTotal);

		writable.appendCommands(Unpooled.wrappedBuffer(PING));
		driveOpenPhaseAction();
		Assert.assertEquals(2L * PING.length, watcher.getSnapshot().getTotalLength());
		Assert.assertSame(opened, watching.getOpenedStore());
		Assert.assertTrue(changes.isEmpty());
	}

	@Test
	public void testPrefixChangeSwitchesStore() throws Exception {
		DefaultReplicationStore writable = createOccupied();
		writable.psyncContinueFrom(REPL_ID, 1);
		DefaultReplicationStore opened = openReadOnly();
		newWatcher();
		CommandStore first = opened.getCommandStore();
		Assert.assertNotNull(first);
		driveOpenPhaseAction();
		Assert.assertSame(opened, watching.getOpenedStore());

		writable.psyncContinueFrom(REPL_ID, 100);
		String newPrefix = writable.getMetaStore().dupReplicationStoreMeta().getCmdFilePrefix();
		driveOpenPhaseAction();

		Assert.assertNull("prefix X -> Y releases the store", watching.getOpenedStore());
		Assert.assertFalse(opened.checkOk());
		Assert.assertEquals(1, changes.size());
		Assert.assertEquals(PrepareStoreWatcher.STORE_SWITCH_REASON_CMD_PREFIX, changes.get(0));
		Assert.assertEquals(1, watcher.getStoreSwitchedCount());
		Assert.assertNull(watcher.getSnapshot());

		DefaultReplicationStore reopened = (DefaultReplicationStore) watching.getCurrent();
		Assert.assertEquals(opened.getBaseDir(), reopened.getBaseDir());
		Assert.assertEquals(newPrefix, ((ReadOnlyCommandStore) reopened.getCommandStore()).getFileNamePrefix());
	}

	@Test
	public void testBuildFailureClosesHandlesAndRetriesNextRound() throws Exception {
		DefaultReplicationStore writable = createOccupied();
		DefaultReplicationStore opened = openReadOnly();
		newWatcher();
		writable.psyncContinueFrom(REPL_ID, 1);
		writable.appendCommands(Unpooled.wrappedBuffer(PING));
		String prefix = writable.getMetaStore().dupReplicationStoreMeta().getCmdFilePrefix();
		doReturn(CompletableFuture.failedFuture(new IOException("inject read-only cmd open fail")))
				.when(fs).open(anyString(), eq(prefix), anyList(), eq(false), anyString());

		driveOpenPhaseAction();
		Assert.assertEquals("build failure stays in closed phase", PrepareStoreWatcher.Phase.CLOSED, watcher.getPhase());
		Assert.assertNull(opened.getCommandStore());
		Assert.assertTrue(opened.isFresh());
		Assert.assertFalse("handles closed like a FS failure", watching.isManagerMetaHandleOpen());
		Assert.assertSame(opened, watching.getOpenedStore());
		Assert.assertTrue(changes.isEmpty());

		doCallRealMethod().when(fs).open(anyString(), eq(prefix), anyList(), eq(false), anyString());
		now.addAndGet(CLOSE_HOLD_MILLI);
		watcher.pollOnce();
		Assert.assertEquals(PrepareStoreWatcher.Phase.OPEN, watcher.getPhase());
		Assert.assertTrue(opened.getCommandStore() instanceof ReadOnlyCommandStore);
		Assert.assertFalse(opened.isFresh());
		Assert.assertEquals(PING.length, watcher.getSnapshot().getTotalLength());
		Assert.assertTrue(changes.isEmpty());
	}

	@Test
	public void testStopWaitsForReopenAndLatePollIgnoresLatestStoreDir() throws Exception {
		DefaultReplicationStore writable = createOccupied();
		writable.psyncContinueFrom(REPL_ID, 1);
		DefaultReplicationStore opened = openReadOnly();
		newWatcher();

		stopWhileReopenIsBlocked();

		DefaultReplicationStore storeB = (DefaultReplicationStore) occupying.create();
		Assert.assertNotEquals(opened.getBaseDir(), storeB.getBaseDir());
		pollToReopenAfterStop();

		Assert.assertSame(opened, watching.getOpenedStore());
		Assert.assertTrue(opened.checkOk());
		Assert.assertTrue(changes.isEmpty());
		Assert.assertEquals(0, watcher.getStoreSwitchedCount());
	}

	@Test
	public void testStopWaitsForReopenAndLatePollDoesNotBuildCmdStore() throws Exception {
		DefaultReplicationStore writable = createOccupied();
		DefaultReplicationStore opened = openReadOnly();
		newWatcher();

		stopWhileReopenIsBlocked();

		writable.psyncContinueFrom(REPL_ID, 1);
		pollToReopenAfterStop();

		Assert.assertNull("no in-place build after stop()", opened.getCommandStore());
		Assert.assertNull("no meta reload after stop()", opened.getMetaStore().getCurrentReplStage());
		Assert.assertSame(opened, watching.getOpenedStore());
		Assert.assertTrue(changes.isEmpty());
	}

	@Test
	public void testLatePollAfterSwitchToWritableLeavesManagerAlone() throws Exception {
		DefaultReplicationStore writable = createOccupied();
		writable.psyncContinueFrom(REPL_ID, 1);
		writable.appendCommands(Unpooled.wrappedBuffer(PING));
		File dir = writable.getBaseDir();
		openReadOnly();
		newWatcher();
		driveOpenPhaseAction();
		Assert.assertNotNull(watcher.getSnapshot());

		watcher.stop();
		LifecycleHelper.stopIfPossible(occupying);
		LifecycleHelper.stopIfPossible(watching);
		watching.setReadOnly(false);
		LifecycleHelper.startIfPossible(watching);
		DefaultReplicationStore reopened = (DefaultReplicationStore) watching.getCurrent();
		Assert.assertEquals(dir, reopened.getBaseDir());
		Properties writerMetaCache = watching.getCurrentMetaCache();
		Assert.assertNotNull(writerMetaCache);
		clearInvocations(watching);

		watcher.pollOnce();
		now.addAndGet(REOPEN_INTERVAL_MILLI);
		watcher.pollOnce();
		now.addAndGet(CLOSE_HOLD_MILLI);
		watcher.pollOnce();

		Assert.assertSame("writer manager meta cache must not be reloaded", writerMetaCache,
				watching.getCurrentMetaCache());
		Assert.assertSame(reopened, watching.getOpenedStore());
		Assert.assertTrue(reopened.checkOk());
		Assert.assertTrue(changes.isEmpty());
		verify(watching, never()).reloadLatestStoreDir();
		verify(watching, never()).releaseCurrentStore();
	}

	/** stop() must wait for a reopen blocked in reloadLatestStoreDir. */
	private void stopWhileReopenIsBlocked() throws Exception {
		watcher.pollOnce();
		now.addAndGet(REOPEN_INTERVAL_MILLI);
		watcher.pollOnce();
		Assert.assertEquals(PrepareStoreWatcher.Phase.CLOSED, watcher.getPhase());
		now.addAndGet(CLOSE_HOLD_MILLI);
		Assert.assertNull(watcher.getSnapshot());

		CountDownLatch entered = new CountDownLatch(1);
		CountDownLatch release = new CountDownLatch(1);
		AtomicBoolean blockOnce = new AtomicBoolean(true);
		doAnswer(inv -> {
			if (blockOnce.getAndSet(false)) {
				entered.countDown();
				release.await();
			}
			return inv.callRealMethod();
		}).when(watching).reloadLatestStoreDir();

		CompletableFuture<Void> poll = CompletableFuture.runAsync(() -> {
			try {
				watcher.pollOnce();
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		});
		Assert.assertTrue(entered.await(FUTURE_WAIT_SECONDS, TimeUnit.SECONDS));
		CompletableFuture<Void> stop = CompletableFuture.runAsync(watcher::stop);
		Thread.sleep(BLOCKED_STOP_CHECK_MILLI);
		Assert.assertFalse("stop() must wait for the reopen in progress", stop.isDone());

		release.countDown();
		stop.get(FUTURE_WAIT_SECONDS, TimeUnit.SECONDS);
		Assert.assertNotNull("stop() returned only after the locked reopen finished", watcher.getSnapshot());
		poll.get(FUTURE_WAIT_SECONDS, TimeUnit.SECONDS);
		doCallRealMethod().when(watching).reloadLatestStoreDir();
		clearInvocations(watching);
	}

	private void pollToReopenAfterStop() throws IOException {
		now.addAndGet(REOPEN_INTERVAL_MILLI);
		watcher.pollOnce();
		Assert.assertEquals(PrepareStoreWatcher.Phase.CLOSED, watcher.getPhase());
		now.addAndGet(CLOSE_HOLD_MILLI);
		watcher.pollOnce();
		Assert.assertEquals("stopped watcher never reopens", PrepareStoreWatcher.Phase.CLOSED, watcher.getPhase());

		verify(watching, never()).reloadLatestStoreDir();
		verify(watching, never()).releaseCurrentStore();
		verify(watching, never()).getCurrent();
	}

	private void driveOpenPhaseAction() throws IOException {
		watcher.pollOnce();
		now.addAndGet(REOPEN_INTERVAL_MILLI);
		watcher.pollOnce();
		now.addAndGet(CLOSE_HOLD_MILLI);
		watcher.pollOnce();
	}

	private DefaultReplicationStore createOccupied() throws Exception {
		LifecycleHelper.initializeIfPossible(occupying);
		LifecycleHelper.startIfPossible(occupying);
		return (DefaultReplicationStore) occupying.create();
	}

	private DefaultReplicationStore openReadOnly() throws Exception {
		LifecycleHelper.initializeIfPossible(watching);
		watching.setReadOnly(true);
		LifecycleHelper.startIfPossible(watching);
		DefaultReplicationStore opened = (DefaultReplicationStore) watching.getCurrent();
		Assert.assertNotNull(opened);
		return opened;
	}

	private void newWatcher() {
		watcher = new PrepareStoreWatcher(watching, keeperConfig, changes::add);
		watcher.setClock(now::get);
	}

	private DefaultReplicationStoreManager newManager(File base, String runid) {
		return new DefaultReplicationStoreManager(keeperConfig, getReplId(), runid, base,
				createkeeperMonitor(), mock(SyncRateManager.class), createRedisOpParser(), fs);
	}

	private void stopDispose(DefaultReplicationStoreManager manager) {
		try {
			LifecycleHelper.stopIfPossible(manager);
		} catch (Throwable th) {
			logger.info("[stopDispose][stop]{}", manager, th);
		}
		try {
			LifecycleHelper.disposeIfPossible(manager);
		} catch (Throwable th) {
			logger.info("[stopDispose][dispose]{}", manager, th);
		}
	}
}

package com.ctrip.xpipe.redis.keeper.prepare;

import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

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
 * Phase WT (T-WT.4): PrepareStoreWatcher. AC-5 / D8 / D11.
 */
public class PrepareStoreWatcherTest extends AbstractRedisKeeperTest {

	private static final String REPL_ID = "000000000000000000000000000000000000000A";

	private static final String REPL_ID_B = "000000000000000000000000000000000000000B";

	private TestKeeperConfig keeperConfig;

	@Before
	public void beforePrepareStoreWatcherTest() {
		keeperConfig = new TestKeeperConfig();
		keeperConfig.setReplicationStoreGcIntervalSeconds(60);
		keeperConfig.setMinTimeMilliToGcAfterCreate(60_000);
		keeperConfig.setPrepareWatchMetaIntervalMilli(50);
	}

	@Test
	public void testStoreDirChangeReleasesAndDisconnectsWithinOnePoll() throws Exception {
		AsyncFileSystem fs = createTestAsyncFileSystem();
		File base = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultReplicationStoreManager occupying = newManager(fs, base, runid);
		DefaultReplicationStoreManager watching = newManager(fs, base, runid);
		List<String> changes = new ArrayList<>();
		AtomicInteger slavesClosed = new AtomicInteger();
		PrepareStoreWatcher watcher = new PrepareStoreWatcher(watching, keeperConfig, reason -> {
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
			watcher.pollOnce();
			Assert.assertEquals(dirA, ((DefaultReplicationStore) watching.getOpenedStore()).getBaseDir());
			Assert.assertEquals(0, watcher.getStoreSwitchedCount());

			DefaultReplicationStore storeB = (DefaultReplicationStore) occupying.create();
			seedCommands(storeB);
			File dirB = storeB.getBaseDir();
			Assert.assertNotEquals(dirA, dirB);

			watcher.pollOnce();

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

	@Test
	public void testDoesNotOpenWhenNotYetOpened() throws Exception {
		AsyncFileSystem fs = createTestAsyncFileSystem();
		File base = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultReplicationStoreManager occupying = newManager(fs, base, runid);
		DefaultReplicationStoreManager watching = newManager(fs, base, runid);
		List<String> changes = new ArrayList<>();
		PrepareStoreWatcher watcher = new PrepareStoreWatcher(watching, keeperConfig, changes::add);
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

			watcher.pollOnce();

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
		PrepareStoreWatcher watcher = new PrepareStoreWatcher(watching, keeperConfig, changes::add);
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
			watcher.pollOnce();
			ReplicationStore opened = watching.getOpenedStore();
			Assert.assertEquals(dir, ((DefaultReplicationStore) opened).getBaseDir());
			Assert.assertEquals(REPL_ID, opened.getMetaStore().getCurrentReplStage().getReplId());

			writable.psyncContinue(REPL_ID_B);
			watcher.pollOnce();

			ReplicationStore still = watching.getOpenedStore();
			Assert.assertSame(opened, still);
			Assert.assertEquals(dir, ((DefaultReplicationStore) still).getBaseDir());
			Assert.assertEquals(0, changes.size());
			Assert.assertEquals(0, watcher.getStoreSwitchedCount());
		} finally {
			watcher.stop();
			stopDispose(watching);
			stopDispose(occupying);
			fs.shutdown();
		}
	}

	@Test
	public void testPollExceptionDoesNotStopNextCycle() throws Exception {
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
			waitConditionUntilTimeOut(() -> watcher.getSnapshot() != null, 2000);
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
			watcher.pollOnce();

			PrepareWatchSnapshot snap = watcher.getSnapshot();
			Assert.assertNotNull(snap);
			clearInvocations(fs);

			long total = snap.getTotalLength();
			long backlog = snap.getBacklogEndOffset();
			Assert.assertEquals(total, backlog);
			Assert.assertTrue(total > 0);

			verify(fs, never()).open(anyString(), any(), anyBoolean(), anyBoolean(), any());
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

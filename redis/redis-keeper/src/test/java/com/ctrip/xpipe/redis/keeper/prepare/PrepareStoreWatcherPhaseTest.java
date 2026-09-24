package com.ctrip.xpipe.redis.keeper.prepare;

import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.store.meta.AbstractMetaStore;
import com.ctrip.xpipe.redis.keeper.store.readonly.ReadOnlyCommandStore;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.mock;

/**
 * Phase HW (T-HW.5): 只读句柄的两相节奏。AC-5b ①②⑤⑥。
 * <p>
 * 全部用注入时钟判定阈值，不睡墙钟、不依赖调度精度 —— 这正是 D48 把两个阈值做成「时钟判定」的目的。
 */
public class PrepareStoreWatcherPhaseTest extends AbstractRedisKeeperTest {

	private static final String REPL_ID = "000000000000000000000000000000000000000A";

	/**
	 * 故意让 {@code close.hold} 大于固定 tick，验证节奏不依赖调度粒度（AC-5b ①）。
	 */
	private static final int REOPEN_INTERVAL_MILLI = 3000;

	private static final int CLOSE_HOLD_MILLI = 2000;

	private static final int DRIVE_TICKS = 40;

	private TestKeeperConfig keeperConfig;

	private final AtomicLong now = new AtomicLong();

	@Before
	public void beforePrepareStoreWatcherPhaseTest() {
		keeperConfig = new TestKeeperConfig();
		keeperConfig.setReplicationStoreGcIntervalSeconds(60);
		keeperConfig.setMinTimeMilliToGcAfterCreate(60_000);
		keeperConfig.setPrepareWatchReopenIntervalMilli(REOPEN_INTERVAL_MILLI);
		keeperConfig.setPrepareWatchCloseHoldMilli(CLOSE_HOLD_MILLI);
		now.set(0);
	}

	/**
	 * AC-5b ①：close→open 的间隔恒 ≥ {@code close.hold}（FS-M5.5 的静置窗口），open→close 的间隔恒
	 * ≥ {@code reopen.interval}，且 {@code close.hold} 大于 tick 时同样成立。
	 */
	@Test
	public void testPhaseGapsNeverViolateCloseHoldNorReopenInterval() throws Exception {
		AsyncFileSystem fs = createTestAsyncFileSystem();
		File base = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultReplicationStoreManager occupying = newManager(fs, base, runid);
		DefaultReplicationStoreManager watching = newManager(fs, base, runid);
		PrepareStoreWatcher watcher = newWatcher(watching);
		try {
			seedOccupied(occupying);
			openReadOnly(watching);

			watcher.pollOnce();
			Assert.assertEquals(PrepareStoreWatcher.Phase.OPEN, watcher.getPhase());

			List<Long> openAt = new ArrayList<>();
			List<Long> closeAt = new ArrayList<>();
			long lastOpenAt = now.get();
			long lastCloseAt = -1;
			PrepareStoreWatcher.Phase prev = watcher.getPhase();
			for (int i = 0; i < DRIVE_TICKS; i++) {
				now.addAndGet(PrepareStoreWatcher.WATCH_TICK_MILLI);
				watcher.pollOnce();
				PrepareStoreWatcher.Phase cur = watcher.getPhase();
				if (prev == PrepareStoreWatcher.Phase.OPEN && cur == PrepareStoreWatcher.Phase.CLOSED) {
					Assert.assertTrue("open phase must last >= reopen.interval, got " + (now.get() - lastOpenAt),
							now.get() - lastOpenAt >= REOPEN_INTERVAL_MILLI);
					lastCloseAt = now.get();
					closeAt.add(lastCloseAt);
				} else if (prev == PrepareStoreWatcher.Phase.CLOSED && cur == PrepareStoreWatcher.Phase.OPEN) {
					Assert.assertTrue("close->open gap must be >= close.hold, got " + (now.get() - lastCloseAt),
							now.get() - lastCloseAt >= CLOSE_HOLD_MILLI);
					lastOpenAt = now.get();
					openAt.add(lastOpenAt);
				}
				prev = cur;
			}

			Assert.assertTrue("expect several close phases, got " + closeAt, closeAt.size() >= 3);
			Assert.assertTrue("expect several open phases, got " + openAt, openAt.size() >= 3);
		} finally {
			watcher.stop();
			stopDispose(watching);
			stopDispose(occupying);
			fs.shutdown();
		}
	}

	/**
	 * AC-5b ②：cmd / {@code meta.v2.json} / {@code store_manager_meta.properties} 三个句柄在同一相位开关。
	 */
	@Test
	public void testThreeReadOnlyHandlesShareTheSamePhase() throws Exception {
		AsyncFileSystem fs = createTestAsyncFileSystem();
		File base = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultReplicationStoreManager occupying = newManager(fs, base, runid);
		DefaultReplicationStoreManager watching = newManager(fs, base, runid);
		PrepareStoreWatcher watcher = newWatcher(watching);
		try {
			seedOccupied(occupying);
			DefaultReplicationStore opened = openReadOnly(watching);

			watcher.pollOnce();
			now.addAndGet(REOPEN_INTERVAL_MILLI);
			watcher.pollOnce();
			Assert.assertEquals(PrepareStoreWatcher.Phase.CLOSED, watcher.getPhase());
			Assert.assertFalse("cmd handle must be closed in closed phase", cmdStore(opened).isHandleOpen());
			Assert.assertFalse("meta.v2.json handle must be closed in closed phase",
					metaStore(opened).isMetaHandleOpen());
			Assert.assertFalse("manager meta handle must be closed in closed phase",
					watching.isManagerMetaHandleOpen());

			now.addAndGet(CLOSE_HOLD_MILLI);
			watcher.pollOnce();
			Assert.assertEquals(PrepareStoreWatcher.Phase.OPEN, watcher.getPhase());
			Assert.assertSame(opened, watching.getOpenedStore());
			Assert.assertTrue("cmd handle must be reopened in open phase", cmdStore(opened).isHandleOpen());
			Assert.assertTrue("meta.v2.json handle must be reopened in open phase",
					metaStore(opened).isMetaHandleOpen());
			Assert.assertTrue("manager meta handle must be reopened in open phase",
					watching.isManagerMetaHandleOpen());
		} finally {
			watcher.stop();
			stopDispose(watching);
			stopDispose(occupying);
			fs.shutdown();
		}
	}

	/**
	 * AC-5b ⑥：请求侧在关相里新开一个 Store 时，Watcher 重绑并从「刚 open」重新计时 ——
	 * 新 Store 的 {@code initialize()} 刚做过全新扫描，本就新鲜，不该被立刻关掉。
	 */
	@Test
	public void testRebindOnNewlyOpenedStoreRestartsOpenPhaseTiming() throws Exception {
		AsyncFileSystem fs = createTestAsyncFileSystem();
		File base = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultReplicationStoreManager occupying = newManager(fs, base, runid);
		DefaultReplicationStoreManager watching = newManager(fs, base, runid);
		PrepareStoreWatcher watcher = newWatcher(watching);
		try {
			seedOccupied(occupying);
			LifecycleHelper.initializeIfPossible(watching);
			watching.setReadOnly(true);
			LifecycleHelper.startIfPossible(watching);
			Assert.assertNull(watching.getOpenedStore());

			// 未打开店：两相只 cycle manager meta 句柄
			watcher.pollOnce();
			now.addAndGet(REOPEN_INTERVAL_MILLI);
			watcher.pollOnce();
			Assert.assertEquals(PrepareStoreWatcher.Phase.CLOSED, watcher.getPhase());
			now.addAndGet(CLOSE_HOLD_MILLI);
			watcher.pollOnce();
			Assert.assertEquals(PrepareStoreWatcher.Phase.OPEN, watcher.getPhase());

			// 请求侧打开新店，随后即使开相已到点，本轮也只重绑、不关句柄
			DefaultReplicationStore opened = (DefaultReplicationStore) watching.getCurrent();
			Assert.assertTrue(cmdStore(opened).isHandleOpen());
			now.addAndGet(REOPEN_INTERVAL_MILLI);
			watcher.pollOnce();
			Assert.assertEquals(PrepareStoreWatcher.Phase.OPEN, watcher.getPhase());
			Assert.assertTrue("freshly opened store must not be cycled immediately",
					cmdStore(opened).isHandleOpen());

			// 重新计时后再满一个 reopen.interval 才进关相
			now.addAndGet(REOPEN_INTERVAL_MILLI);
			watcher.pollOnce();
			Assert.assertEquals(PrepareStoreWatcher.Phase.CLOSED, watcher.getPhase());
			Assert.assertFalse(cmdStore(opened).isHandleOpen());
		} finally {
			watcher.stop();
			stopDispose(watching);
			stopDispose(occupying);
			fs.shutdown();
		}
	}

	/**
	 * AC-5b ⑤：无 Reader 时照常 cycle —— 源码门禁 Watcher 不以 {@code hasReaders()} 作为 gate。
	 */
	@Test
	public void testWatcherDoesNotGateOnHasReaders() throws Exception {
		File source = new File(
				"src/main/java/com/ctrip/xpipe/redis/keeper/prepare/PrepareStoreWatcher.java");
		Assert.assertTrue(source.isFile());
		String text = stripComments(new String(Files.readAllBytes(source.toPath()), StandardCharsets.UTF_8));
		Assert.assertFalse("Watcher must not gate the cycle on hasReaders() (D48)", text.contains("hasReaders"));
		Assert.assertTrue(text.contains("closeHoldMilli"));
		Assert.assertTrue(text.contains("reopenIntervalMilli"));
	}

	private PrepareStoreWatcher newWatcher(DefaultReplicationStoreManager manager) {
		PrepareStoreWatcher watcher = new PrepareStoreWatcher(manager, keeperConfig, PrepareStoreChangeListener.NOOP);
		watcher.setClock(now::get);
		return watcher;
	}

	private void seedOccupied(DefaultReplicationStoreManager occupying) throws Exception {
		LifecycleHelper.initializeIfPossible(occupying);
		LifecycleHelper.startIfPossible(occupying);
		DefaultReplicationStore writable = (DefaultReplicationStore) occupying.create();
		writable.psyncContinueFrom(REPL_ID, 1);
		writable.appendCommands(Unpooled.wrappedBuffer("*1\r\n$4\r\nPING\r\n".getBytes()));
	}

	private DefaultReplicationStore openReadOnly(DefaultReplicationStoreManager watching) throws Exception {
		LifecycleHelper.initializeIfPossible(watching);
		watching.setReadOnly(true);
		LifecycleHelper.startIfPossible(watching);
		ReplicationStore store = watching.getCurrent();
		Assert.assertNotNull(store);
		return (DefaultReplicationStore) store;
	}

	private static ReadOnlyCommandStore cmdStore(DefaultReplicationStore store) {
		CommandStore cmdStore = store.getCommandStore();
		Assert.assertTrue("read-only store must use ReadOnlyCommandStore, got " + cmdStore,
				cmdStore instanceof ReadOnlyCommandStore);
		return (ReadOnlyCommandStore) cmdStore;
	}

	private static AbstractMetaStore metaStore(DefaultReplicationStore store) {
		Assert.assertTrue(store.getMetaStore() instanceof AbstractMetaStore);
		return (AbstractMetaStore) store.getMetaStore();
	}

	private DefaultReplicationStoreManager newManager(AsyncFileSystem fs, File base, String runid) {
		return new DefaultReplicationStoreManager(
				keeperConfig, getReplId(), runid, base,
				createkeeperMonitor(), mock(SyncRateManager.class), createRedisOpParser(), null, fs);
	}

	private static String stripComments(String source) {
		String noBlock = source.replaceAll("/\\*[\\s\\S]*?\\*/", "");
		StringBuilder out = new StringBuilder(noBlock.length());
		for (String line : noBlock.split("\n", -1)) {
			int idx = line.indexOf("//");
			out.append(idx >= 0 ? line.substring(0, idx) : line).append('\n');
		}
		return out.toString();
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

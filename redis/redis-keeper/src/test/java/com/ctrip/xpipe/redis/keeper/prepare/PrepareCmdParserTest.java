package com.ctrip.xpipe.redis.keeper.prepare;

import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.config.KeeperPubSubConstants;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.LogCapture;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.logging.log4j.Level;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.mock;

/**
 * Phase PH (T-PH.4 ②③): PREPARE Parser 自己 getCurrent()；换店后 Watcher 仍轮询。AC-7b / AC-10。
 */
public class PrepareCmdParserTest extends AbstractRedisKeeperTest {

	private static final String REPL_ID = "000000000000000000000000000000000000000A";

	/**
	 * Watcher 的 tick 固定 1s（D48），一次「关相 → 静置 → 开相」至少跨两个 tick，因此任何依赖
	 * 「只读侧看到新字节」的等待都必须按 tick 的倍数给上界，不能沿用返工前的 3s。
	 */
	private static final int WATCH_CYCLE_WAIT_MILLI = 6 * PrepareStoreWatcher.WATCH_TICK_MILLI;

	private TestKeeperConfig keeperConfig;

	@Before
	public void beforePrepareCmdParserTest() {
		keeperConfig = new TestKeeperConfig();
		keeperConfig.setReplicationStoreGcIntervalSeconds(60);
		keeperConfig.setMinTimeMilliToGcAfterCreate(60_000);
		keeperConfig.setPrepareWatchReopenIntervalMilli(50);
		keeperConfig.setPrepareWatchCloseHoldMilli(10);
	}

	@Test
	public void testPrepareParserReadsPublishAfterOwnGetCurrent() throws Exception {
		AsyncFileSystem fs = createTestAsyncFileSystem();
		File base = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultReplicationStoreManager occupying = newManager(fs, base, runid);
		DefaultReplicationStoreManager watching = newManager(fs, base, runid);
		List<String> received = new CopyOnWriteArrayList<>();
		PrepareCmdParser parser = new PrepareCmdParser(watching, createRedisOpParser(),
				(channel, message) -> received.add(channel + ":" + message));
		// D48：只读句柄的开关与 totalLength 观察只由 Watcher 驱动，Reader 自己不 reopen，
		// 所以 Parser 的闭环必须和生产一样带上 Watcher（doBecomePrepare 先起 watcher 再起 parser）
		PrepareStoreWatcher watcher = new PrepareStoreWatcher(watching, keeperConfig,
				PrepareStoreChangeListener.NOOP);
		try {
			LifecycleHelper.initializeIfPossible(occupying);
			LifecycleHelper.startIfPossible(occupying);
			DefaultReplicationStore storeA = (DefaultReplicationStore) occupying.create();
			storeA.psyncContinueFrom(REPL_ID, 1);
			storeA.appendCommands(Unpooled.wrappedBuffer("*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII)));

			LifecycleHelper.initializeIfPossible(watching);
			watching.setReadOnly(true);
			LifecycleHelper.startIfPossible(watching);
			watcher.start();
			parser.start();
			waitConditionUntilTimeOut(() -> parser.getAttachCount() >= 1);

			storeA.appendCommands(publishBuf("delay", "t1"));
			waitConditionUntilTimeOut(() -> received.contains("delay:t1"), WATCH_CYCLE_WAIT_MILLI);
			Assert.assertEquals(1, received.size());
			Assert.assertTrue(countParserThreads() >= 1);
		} finally {
			parser.stop();
			watcher.stop();
			stopDispose(watching);
			stopDispose(occupying);
			fs.shutdown();
		}
	}

	@Test
	public void testStoreSwitchReleasesWhileParserReopensAndWatcherKeepsPolling() throws Exception {
		AsyncFileSystem fs = createTestAsyncFileSystem();
		File base = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultReplicationStoreManager occupying = newManager(fs, base, runid);
		DefaultReplicationStoreManager watching = newManager(fs, base, runid);
		List<String> changes = new ArrayList<>();
		PrepareStoreWatcher watcher = new PrepareStoreWatcher(watching, keeperConfig, changes::add);
		PrepareCmdParser parser = new PrepareCmdParser(watching, createRedisOpParser(),
				(channel, message) -> { });
		try {
			LifecycleHelper.initializeIfPossible(occupying);
			LifecycleHelper.startIfPossible(occupying);
			DefaultReplicationStore storeA = (DefaultReplicationStore) occupying.create();
			storeA.psyncContinueFrom(REPL_ID, 1);
			storeA.appendCommands(Unpooled.wrappedBuffer("*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII)));
			File dirA = storeA.getBaseDir();

			LifecycleHelper.initializeIfPossible(watching);
			watching.setReadOnly(true);
			LifecycleHelper.startIfPossible(watching);
			watcher.start();
			parser.start();
			waitConditionUntilTimeOut(() -> parser.getAttachCount() >= 1);
			waitConditionUntilTimeOut(() -> watcher.getPollCount() >= 1);
			Assert.assertEquals(dirA, ((DefaultReplicationStore) watching.getOpenedStore()).getBaseDir());

			long pollsBefore = watcher.getPollCount();
			DefaultReplicationStore storeB = (DefaultReplicationStore) occupying.create();
			storeB.psyncContinueFrom(REPL_ID, 1);
			storeB.appendCommands(Unpooled.wrappedBuffer("*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII)));
			File dirB = storeB.getBaseDir();
			Assert.assertNotEquals(dirA, dirB);

			waitConditionUntilTimeOut(() -> watching.getOpenedStore() == null, WATCH_CYCLE_WAIT_MILLI);
			Assert.assertEquals(1, changes.size());
			waitConditionUntilTimeOut(() -> parser.getAttachCount() >= 2, WATCH_CYCLE_WAIT_MILLI);
			waitConditionUntilTimeOut(() -> watching.getOpenedStore() != null
					&& dirB.equals(((DefaultReplicationStore) watching.getOpenedStore()).getBaseDir()), WATCH_CYCLE_WAIT_MILLI);
			waitConditionUntilTimeOut(() -> watcher.getPollCount() > pollsBefore, WATCH_CYCLE_WAIT_MILLI);
			Assert.assertTrue(countParserThreads() >= 1);
			Assert.assertTrue(countWatchThreads() >= 1);
		} finally {
			parser.stop();
			watcher.stop();
			stopDispose(watching);
			stopDispose(occupying);
			fs.shutdown();
		}
	}

	/** keeper-tfs-m6 T-P12.5 (parser) */
	@Test
	public void testFreshStoreRetriesQuietlyAndAttachesAfterInPlaceBuild() throws Exception {
		AsyncFileSystem fs = createTestAsyncFileSystem();
		File base = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultReplicationStoreManager occupying = newManager(fs, base, runid);
		DefaultReplicationStoreManager watching = newManager(fs, base, runid);
		List<String> received = new CopyOnWriteArrayList<>();
		PrepareCmdParser parser = new PrepareCmdParser(watching, createRedisOpParser(),
				(channel, message) -> received.add(channel + ":" + message));
		AtomicLong now = new AtomicLong();
		PrepareStoreWatcher watcher = new PrepareStoreWatcher(watching, keeperConfig,
				PrepareStoreChangeListener.NOOP);
		watcher.setClock(now::get);
		try (LogCapture parserLogs = new LogCapture(PrepareCmdParser.class)) {
			LifecycleHelper.initializeIfPossible(occupying);
			LifecycleHelper.startIfPossible(occupying);
			DefaultReplicationStore writable = (DefaultReplicationStore) occupying.create();

			LifecycleHelper.initializeIfPossible(watching);
			watching.setReadOnly(true);
			LifecycleHelper.startIfPossible(watching);
			parser.start();
			waitConditionUntilTimeOut(() -> watching.getOpenedStore() != null);
			DefaultReplicationStore opened = (DefaultReplicationStore) watching.getOpenedStore();
			Thread.sleep(3 * KeeperPubSubConstants.PREPARE_CMD_PARSER_RETRY_MILLI);
			Assert.assertEquals(0, parser.getAttachCount());

			writable.psyncContinueFrom(REPL_ID, 1);
			writable.appendCommands(Unpooled.wrappedBuffer("*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII)));
			Thread.sleep(3 * KeeperPubSubConstants.PREPARE_CMD_PARSER_RETRY_MILLI);
			Assert.assertEquals("meta not reloaded yet: still fresh", 0, parser.getAttachCount());

			driveWatcherCycle(watcher, now);
			Assert.assertSame(opened, watching.getOpenedStore());
			Assert.assertNotNull(opened.getCommandStore());
			waitConditionUntilTimeOut(() -> parser.getAttachCount() >= 1);

			writable.appendCommands(publishBuf("delay", "t1"));
			waitConditionUntilTimeOut(() -> {
				try {
					driveWatcherCycle(watcher, now);
				} catch (IOException e) {
					throw new IllegalStateException(e);
				}
				return received.contains("delay:t1");
			}, WATCH_CYCLE_WAIT_MILLI);
			Assert.assertEquals(1, parser.getAttachCount());
			Assert.assertTrue("fresh store must be retried without WARN: " + parserLogs.atLeast(Level.WARN),
					parserLogs.atLeast(Level.WARN).isEmpty());
		} finally {
			parser.stop();
			watcher.stop();
			stopDispose(watching);
			stopDispose(occupying);
			fs.shutdown();
		}
	}

	private void driveWatcherCycle(PrepareStoreWatcher watcher, AtomicLong now) throws IOException {
		watcher.pollOnce();
		now.addAndGet(keeperConfig.getPrepareWatchReopenIntervalMilli());
		watcher.pollOnce();
		now.addAndGet(keeperConfig.getPrepareWatchCloseHoldMilli());
		watcher.pollOnce();
	}

	@Test
	public void testStopDoesNotJoinParserThread() throws Exception {
		AsyncFileSystem fs = createTestAsyncFileSystem();
		File base = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultReplicationStoreManager occupying = newManager(fs, base, runid);
		DefaultReplicationStoreManager watching = newManager(fs, base, runid);
		PrepareCmdParser parser = new PrepareCmdParser(watching, createRedisOpParser(),
				(channel, message) -> { });
		try {
			LifecycleHelper.initializeIfPossible(occupying);
			LifecycleHelper.startIfPossible(occupying);
			DefaultReplicationStore storeA = (DefaultReplicationStore) occupying.create();
			storeA.psyncContinueFrom(REPL_ID, 1);
			storeA.appendCommands(Unpooled.wrappedBuffer("*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII)));

			LifecycleHelper.initializeIfPossible(watching);
			watching.setReadOnly(true);
			LifecycleHelper.startIfPossible(watching);
			parser.start();
			waitConditionUntilTimeOut(() -> parser.getAttachCount() >= 1);

			long begin = System.currentTimeMillis();
			parser.stop();
			long elapsed = System.currentTimeMillis() - begin;
			Assert.assertFalse(parser.isRunning());
			Assert.assertTrue("stop joined parser thread, elapsed=" + elapsed, elapsed < 200);
		} finally {
			parser.stop();
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

	private static ByteBuf publishBuf(String channel, String message) {
		String body = "*3\r\n$7\r\nPUBLISH\r\n$" + channel.length() + "\r\n" + channel
				+ "\r\n$" + message.length() + "\r\n" + message + "\r\n";
		return Unpooled.wrappedBuffer(body.getBytes(StandardCharsets.US_ASCII));
	}

	private static long countParserThreads() {
		return Thread.getAllStackTraces().keySet().stream()
				.filter(t -> t.getName() != null && t.getName().contains("prepare-cmd-parser"))
				.count();
	}

	private static long countWatchThreads() {
		return Thread.getAllStackTraces().keySet().stream()
				.filter(t -> t.getName() != null && t.getName().contains("prepare-watch"))
				.count();
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

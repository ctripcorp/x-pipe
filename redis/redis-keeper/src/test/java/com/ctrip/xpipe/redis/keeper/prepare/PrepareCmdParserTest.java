package com.ctrip.xpipe.redis.keeper.prepare;

import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.mockito.Mockito.mock;

/**
 * Phase PH (T-PH.4 ②③): PREPARE Parser 自己 getCurrent()；换店后 Watcher 仍轮询。AC-7b / AC-10。
 */
public class PrepareCmdParserTest extends AbstractRedisKeeperTest {

	private static final String REPL_ID = "000000000000000000000000000000000000000A";

	private TestKeeperConfig keeperConfig;

	@Before
	public void beforePrepareCmdParserTest() {
		keeperConfig = new TestKeeperConfig();
		keeperConfig.setReplicationStoreGcIntervalSeconds(60);
		keeperConfig.setMinTimeMilliToGcAfterCreate(60_000);
		keeperConfig.setPrepareWatchMetaIntervalMilli(50);
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

			storeA.appendCommands(publishBuf("delay", "t1"));
			waitConditionUntilTimeOut(() -> received.contains("delay:t1"), 3000);
			Assert.assertEquals(1, received.size());
			Assert.assertTrue(countParserThreads() >= 1);
		} finally {
			parser.stop();
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

			waitConditionUntilTimeOut(() -> watching.getOpenedStore() == null, 3000);
			Assert.assertEquals(1, changes.size());
			waitConditionUntilTimeOut(() -> parser.getAttachCount() >= 2, 3000);
			waitConditionUntilTimeOut(() -> watching.getOpenedStore() != null
					&& dirB.equals(((DefaultReplicationStore) watching.getOpenedStore()).getBaseDir()), 3000);
			waitConditionUntilTimeOut(() -> watcher.getPollCount() > pollsBefore, 3000);
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

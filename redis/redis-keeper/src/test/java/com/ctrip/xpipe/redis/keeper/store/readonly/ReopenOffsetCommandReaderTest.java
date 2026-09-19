package com.ctrip.xpipe.redis.keeper.store.readonly;

import com.ctrip.xpipe.redis.core.store.CommandsListener;
import com.ctrip.xpipe.redis.core.store.OffsetReplicationProgress;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.store.AbstractCommandStore;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.Closeable;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.withSettings;

@RunWith(MockitoJUnitRunner.class)
public class ReopenOffsetCommandReaderTest {

	private static final File CMD_FILE = new File("/data/repl_1/store_abc/cmd_");

	@Mock
	private AsyncFileSystem asyncFileSystem;

	@Mock
	private AsyncSegmentFile listingHandle;

	@Mock
	private AsyncSegmentFile readHandle1;

	@Mock
	private AsyncSegmentFile readHandle2;

	private ReadOnlyCommandStore store;

	private CommandsListener listener;

	private Closeable closeableListener;

	private final AtomicInteger sleepCount = new AtomicInteger();

	private final AtomicLong now = new AtomicLong(0);

	private final AtomicLong lastSleepMilli = new AtomicLong();

	private final AtomicReference<String> sleepThreadName = new AtomicReference<>();

	private final ReadOnlyCommandStore.BackoffSleeper sleeper = millis -> {
		sleepCount.incrementAndGet();
		lastSleepMilli.set(millis);
		sleepThreadName.set(Thread.currentThread().getName());
	};

	@Before
	public void setUp() throws Exception {
		now.set(0);
		sleepCount.set(0);
		lastSleepMilli.set(0);
		sleepThreadName.set(null);
		listener = Mockito.mock(CommandsListener.class, withSettings().extraInterfaces(Closeable.class));
		closeableListener = (Closeable) listener;
		Mockito.lenient().when(listener.isOpen()).thenReturn(true);

		Mockito.lenient().when(asyncFileSystem.open(Mockito.anyString(), Mockito.anyString(), Mockito.anyList(),
						Mockito.eq(false), Mockito.anyString()))
				.thenReturn(CompletableFuture.completedFuture(listingHandle),
						CompletableFuture.completedFuture(readHandle1),
						CompletableFuture.completedFuture(readHandle2));
		Mockito.lenient().when(asyncFileSystem.close(Mockito.any(AsyncSegmentFile.class)))
				.thenReturn(CompletableFuture.completedFuture(null));
		Mockito.lenient().when(asyncFileSystem.getStartOffsetByReadOffset(Mockito.any(), Mockito.anyLong()))
				.thenReturn(0L);

		store = new ReadOnlyCommandStore(CMD_FILE, asyncFileSystem, ReplId.from(1L));
		store.setMilliClock(now::get);
		store.setBackoffSleeper(sleeper);
		store.initialize();
	}

	@Test
	public void testWatcherCycleSeesNewBytesAfterWriteSideAppend() throws Exception {
		Mockito.when(asyncFileSystem.list(readHandle1)).thenReturn(Collections.singletonList(0L));
		Mockito.when(asyncFileSystem.sizeOfSegment(readHandle1, 0L))
				.thenReturn(CompletableFuture.completedFuture(8L));
		Mockito.when(asyncFileSystem.read(readHandle1, 8L, 0L))
				.thenReturn(CompletableFuture.completedFuture(Unpooled.wrappedBuffer(new byte[8])));

		ReopenOffsetCommandReader reader = newReader(0);
		Assert.assertNull(reader.read(10));
		Assert.assertEquals(0, sleepCount.get());
		Assert.assertEquals(0L, store.totalLength());

		// 关相：句柄不在，Reader 只 return null，不 reopen
		store.closeHandleForCycle();
		Assert.assertNull(reader.read(10));
		Assert.assertEquals(0, sleepCount.get());

		// 开相：Watcher 独占驱动 open + 观察
		Assert.assertEquals(ReadOnlyCommandStore.ObserveResult.OPENED, store.openAndObserve());
		Assert.assertEquals(8L, store.totalLength());

		ByteBuf buf = reader.read(10);
		Assert.assertNotNull(buf);
		Assert.assertEquals(8, buf.readableBytes());
		Assert.assertSame(readHandle1, store.getAsyncSegmentFile());
		buf.release();
		reader.close();
	}

	@Test
	public void testZeroByteMissBacksOffWithoutRebuildOrDisconnect() throws Exception {
		Mockito.when(listener.isOpen()).thenAnswer(invocation -> sleepCount.get() < 2);

		store.addCommandsListener(new OffsetReplicationProgress(0), listener);

		Assert.assertEquals(2, sleepCount.get());
		Assert.assertEquals(ReadOnlyCommandStore.MISS_BACKOFF_MILLI, lastSleepMilli.get());
		Mockito.verify(closeableListener, Mockito.never()).close();
		// Reader 既不 reopen 也不换句柄
		Assert.assertSame(listingHandle, store.getAsyncSegmentFile());
		Mockito.verify(asyncFileSystem, Mockito.times(1)).open(
				Mockito.anyString(), Mockito.anyString(), Mockito.anyList(), Mockito.eq(false), Mockito.anyString());
	}

	@Test
	public void testReadFailThrowsWithoutClosingListener() throws Exception {
		Mockito.when(asyncFileSystem.list(readHandle1)).thenReturn(Collections.singletonList(0L));
		Mockito.when(asyncFileSystem.sizeOfSegment(readHandle1, 0L))
				.thenReturn(CompletableFuture.completedFuture(8L));
		Mockito.when(asyncFileSystem.read(readHandle1, 8L, 0L))
				.thenReturn(CompletableFuture.failedFuture(new RuntimeException("read fail")));
		store.closeHandleForCycle();
		Assert.assertEquals(ReadOnlyCommandStore.ObserveResult.OPENED, store.openAndObserve());

		ReopenOffsetCommandReader reader = newReader(0);
		try {
			reader.read(10);
			Assert.fail("expected IOException");
		} catch (java.io.IOException expected) {
			Assert.assertTrue(expected.getMessage().contains("read-only command")
					|| expected.getCause() != null);
		}
		Mockito.verify(closeableListener, Mockito.never()).close();
		reader.close();
	}

	/**
	 * AC-5b ④：关相里 Reader 与「追上可见尾」走同一条路 —— 按 {@code MISS_BACKOFF_MILLI} 退避，
	 * 不 reopen、不等唤醒；Watcher 进开相后下一次退避结束即恢复消费。
	 */
	@Test
	public void testClosedPhaseBacksOffWithSameMissPathAndResumesAfterOpen() throws Exception {
		Mockito.when(asyncFileSystem.list(readHandle1)).thenReturn(Collections.singletonList(0L));
		Mockito.when(asyncFileSystem.sizeOfSegment(readHandle1, 0L))
				.thenReturn(CompletableFuture.completedFuture(4L));
		Mockito.when(asyncFileSystem.read(readHandle1, 4L, 0L))
				.thenReturn(CompletableFuture.completedFuture(Unpooled.wrappedBuffer(new byte[4])));

		// 关相：句柄不在，Reader 读到 null
		store.closeHandleForCycle();
		Assert.assertFalse(store.isHandleOpen());

		// 退避的第二轮里模拟 Watcher 进开相，验证 Reader 无需被唤醒也能恢复
		store.setBackoffSleeper(millis -> {
			sleepCount.incrementAndGet();
			lastSleepMilli.set(millis);
			if (sleepCount.get() == 2) {
				store.openAndObserve();
			}
		});

		AtomicInteger deliveredBytes = new AtomicInteger();
		Mockito.when(listener.isOpen()).thenAnswer(invocation -> deliveredBytes.get() == 0);
		Mockito.when(listener.onCommand(Mockito.any())).thenAnswer(invocation -> {
			ByteBuf buf = invocation.getArgument(0);
			deliveredBytes.set(buf.readableBytes());
			return null;
		});

		store.addCommandsListener(new OffsetReplicationProgress(0), listener);

		Assert.assertEquals(2, sleepCount.get());
		Assert.assertEquals(ReadOnlyCommandStore.MISS_BACKOFF_MILLI, lastSleepMilli.get());
		Assert.assertEquals(4, deliveredBytes.get());
		Assert.assertTrue(store.isHandleOpen());
		Mockito.verify(closeableListener, Mockito.never()).close();
	}

	/**
	 * AC-7b：退避不持 {@code handleLock} —— 退避中的 Reader 不阻塞 Watcher 关 / 开句柄。
	 */
	@Test
	public void testBackoffDoesNotBlockWatcherCyclingHandle() throws Exception {
		Mockito.when(asyncFileSystem.list(readHandle1)).thenReturn(Collections.singletonList(0L));
		Mockito.when(asyncFileSystem.sizeOfSegment(readHandle1, 0L))
				.thenReturn(CompletableFuture.completedFuture(0L));

		AtomicReference<Exception> watcherError = new AtomicReference<>();
		store.setBackoffSleeper(millis -> {
			sleepCount.incrementAndGet();
			// 退避期间另一条线程（Watcher）关句柄再 open，不得被挡住
			Thread watcher = new Thread(() -> {
				try {
					store.closeHandleForCycle();
					store.openAndObserve();
				} catch (Exception e) {
					watcherError.set(e);
				}
			}, "prepare-watch-repl_1");
			watcher.start();
			watcher.join(5000);
			Assert.assertFalse("watcher blocked by reader backoff", watcher.isAlive());
		});

		Mockito.when(listener.isOpen()).thenAnswer(invocation -> sleepCount.get() < 1);
		runOnThread("psync-repl_1",
				() -> store.addCommandsListener(new OffsetReplicationProgress(0), listener));

		Assert.assertNull(watcherError.get());
		Assert.assertEquals(1, sleepCount.get());
		Assert.assertTrue(store.isHandleOpen());
		Mockito.verify(closeableListener, Mockito.never()).close();
	}

	/**
	 * AC-7b / D18c：退避睡在调用 {@code addCommandsListener} 的那条线程上 —— 该循环本身就是阻塞的，
	 * 线程归属由调用方保证（slave 的 psync executor / `PrepareCmdParser` 线程），Store 内不做运行时判定。
	 */
	@Test
	public void testBackoffSleepsOnCallerThread() throws Exception {
		runOnThread("psync-repl_1", () -> store.missAndBackoff());
		Assert.assertEquals(1, sleepCount.get());
		Assert.assertEquals("psync-repl_1", sleepThreadName.get());
	}

	@Test
	public void testAddCommandsListenerDeliversAfterWatcherCycle() throws Exception {
		Mockito.when(asyncFileSystem.list(readHandle1)).thenReturn(Collections.singletonList(0L));
		Mockito.when(asyncFileSystem.sizeOfSegment(readHandle1, 0L))
				.thenReturn(CompletableFuture.completedFuture(4L));
		Mockito.when(asyncFileSystem.read(readHandle1, 4L, 0L))
				.thenReturn(CompletableFuture.completedFuture(Unpooled.wrappedBuffer(new byte[4])));

		AtomicBoolean delivered = new AtomicBoolean();
		AtomicInteger deliveredBytes = new AtomicInteger();
		Mockito.when(listener.isOpen()).thenAnswer(invocation -> !delivered.get());
		Mockito.when(listener.onCommand(Mockito.any())).thenAnswer(invocation -> {
			ByteBuf buf = invocation.getArgument(0);
			deliveredBytes.set(buf.readableBytes());
			delivered.set(true);
			return null;
		});

		// 退避期间由 Watcher（这里用 sleeper 回调模拟其线程）走一次关相 → 开相
		store.setBackoffSleeper(millis -> {
			sleepCount.incrementAndGet();
			lastSleepMilli.set(millis);
			if (sleepCount.get() == 1) {
				store.closeHandleForCycle();
				store.openAndObserve();
			}
		});

		store.addCommandsListener(new OffsetReplicationProgress(0), listener);

		Assert.assertEquals(1, sleepCount.get());
		Assert.assertEquals(4, deliveredBytes.get());
		Mockito.verify(closeableListener, Mockito.never()).close();
		Assert.assertEquals(4L, store.totalLength());
	}

	@Test
	public void testReaderNeverOpensOrClosesHandle() throws Exception {
		Mockito.when(asyncFileSystem.list(readHandle1)).thenReturn(Collections.singletonList(0L));
		Mockito.when(asyncFileSystem.sizeOfSegment(readHandle1, 0L))
				.thenReturn(CompletableFuture.completedFuture(8L));

		store.closeHandleForCycle();
		Assert.assertEquals(ReadOnlyCommandStore.ObserveResult.OPENED, store.openAndObserve());
		Assert.assertSame(readHandle1, store.getAsyncSegmentFile());

		// 关相里 Reader 仍有 visible（快照保留），但 readAt 返回 null 且不自行 open
		store.closeHandleForCycle();
		ReopenOffsetCommandReader reader = newReader(0);
		Assert.assertEquals(8L, store.totalLength());
		Assert.assertNull(reader.read(10));
		reader.close();

		// open：initialize + 一次开相；close：两次关相
		Mockito.verify(asyncFileSystem, Mockito.times(2)).open(
				Mockito.anyString(), Mockito.anyString(), Mockito.anyList(), Mockito.eq(false), Mockito.anyString());
		Mockito.verify(asyncFileSystem, Mockito.times(1)).close(listingHandle);
		Mockito.verify(asyncFileSystem, Mockito.times(1)).close(readHandle1);
	}

	@Test
	public void testAddCommandsListenerRethrowsWithoutClosingListener() throws Exception {
		Mockito.when(asyncFileSystem.list(readHandle1)).thenReturn(Collections.singletonList(0L));
		Mockito.when(asyncFileSystem.sizeOfSegment(readHandle1, 0L))
				.thenReturn(CompletableFuture.completedFuture(8L));
		Mockito.when(asyncFileSystem.read(readHandle1, 8L, 0L))
				.thenReturn(CompletableFuture.failedFuture(new RuntimeException("read fail")));
		store.closeHandleForCycle();
		Assert.assertEquals(ReadOnlyCommandStore.ObserveResult.OPENED, store.openAndObserve());

		try {
			store.addCommandsListener(new OffsetReplicationProgress(0), listener);
			Assert.fail("expected IOException");
		} catch (java.io.IOException expected) {
			Assert.assertTrue(expected.getMessage().contains("read-only command")
					|| expected.getCause() != null);
		}
		Mockito.verify(closeableListener, Mockito.never()).close();
	}

	@Test
	public void testDoesNotOpenPerReaderHandleOrUseTransferTo() throws Exception {
		File readerSource = new File(
				"src/main/java/com/ctrip/xpipe/redis/keeper/store/readonly/ReopenOffsetCommandReader.java");
		File storeSource = new File(
				"src/main/java/com/ctrip/xpipe/redis/keeper/store/readonly/ReadOnlyCommandStore.java");
		Assert.assertTrue(readerSource.isFile());
		Assert.assertTrue(storeSource.isFile());
		String readerText = stripComments(new String(Files.readAllBytes(readerSource.toPath()), StandardCharsets.UTF_8));
		String storeText = stripComments(new String(Files.readAllBytes(storeSource.toPath()), StandardCharsets.UTF_8));
		Assert.assertFalse(readerText.contains("OffsetNotifier"));
		Assert.assertFalse(storeText.contains("OffsetNotifier"));
		Assert.assertFalse(readerText.contains("extends OffsetCommandReader"));
		Assert.assertFalse(readerText.contains("AsyncReferenceFileRegion"));
		Assert.assertFalse(readerText.contains("transferTo"));
		Assert.assertFalse(readerText.contains("getPsyncLimitPerSecond"));
		Assert.assertFalse(readerText.contains("endPositionExcluded"));
		Assert.assertFalse(readerText.contains("disconnectListener"));
		Assert.assertFalse(readerText.contains("fs.open"));
		Assert.assertFalse(readerText.contains("Thread.sleep"));
		Assert.assertFalse(readerText.contains("BackoffSleeper"));
		Assert.assertFalse(readerText.contains("EventMonitor"));
		// D48：句柄开关只由 Watcher 驱动，Reader 不碰
		Assert.assertFalse(readerText.contains("openAndObserve"));
		Assert.assertFalse(readerText.contains("closeHandleForCycle"));
		Assert.assertTrue(storeText.contains("readAt"));
		Assert.assertTrue(storeText.contains("missAndBackoff"));
		// D9 ①：读不到数据只退避，不做 wait / notify 唤醒
		Assert.assertFalse(storeText.contains("phaseMonitor"));
		Assert.assertFalse(storeText.contains("notifyAll"));
		Assert.assertFalse(storeText.contains("awaitHandleOpen"));
		Assert.assertFalse(readerText.contains("isHandleOpen"));
		// D18c：Store 不按线程名做运行时判定（keeper 的 event loop 叫 boss-/work-/master-，按名字匹配本就不成立）
		Assert.assertFalse(storeText.contains("Thread.currentThread().getName()"));
		// AC-7b：阻塞循环的线程归属由调用方保证 —— slave 走 psync executor，PREPARE 走 parser 自己的线程
		Assert.assertTrue(stripComments(readSource(
				"src/main/java/com/ctrip/xpipe/redis/keeper/handler/keeper/GapAllowSyncHandler.java"))
				.contains("processPsyncSequentially"));
		Assert.assertTrue(stripComments(readSource(
				"src/main/java/com/ctrip/xpipe/redis/keeper/prepare/PrepareCmdParser.java"))
				.contains("\"prepare-cmd-parser\""));
	}

	private static String readSource(String path) throws java.io.IOException {
		File source = new File(path);
		Assert.assertTrue(path, source.isFile());
		return new String(Files.readAllBytes(source.toPath()), StandardCharsets.UTF_8);
	}

	private ReopenOffsetCommandReader newReader(long offset) {
		return new ReopenOffsetCommandReader(offset, store,
				AbstractCommandStore.DEFAULT_COMMAND_READER_FLYING_THRESHOLD);
	}

	private static void runOnThread(String name, ThrowingRunnable action) throws Exception {
		AtomicReference<Exception> error = new AtomicReference<>();
		CountDownLatch done = new CountDownLatch(1);
		Thread thread = new Thread(() -> {
			try {
				action.run();
			} catch (Exception e) {
				error.set(e);
			} finally {
				done.countDown();
			}
		}, name);
		thread.start();
		Assert.assertTrue(done.await(5, TimeUnit.SECONDS));
		if (error.get() != null) {
			throw error.get();
		}
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

	@FunctionalInterface
	private interface ThrowingRunnable {
		void run() throws Exception;
	}
}

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

	private final ReadOnlyCommandStore.ReopenSleeper sleeper = millis -> {
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
		store.setReopenSleeper(sleeper);
		store.initialize();
	}

	@Test
	public void testReopenSeesNewBytesAfterWriteSideAppend() throws Exception {
		Mockito.when(asyncFileSystem.list(readHandle1)).thenReturn(Collections.singletonList(0L));
		Mockito.when(asyncFileSystem.sizeOfSegment(readHandle1, 0L))
				.thenReturn(CompletableFuture.completedFuture(0L));
		Mockito.when(asyncFileSystem.list(readHandle2)).thenReturn(Collections.singletonList(0L));
		Mockito.when(asyncFileSystem.sizeOfSegment(readHandle2, 0L))
				.thenReturn(CompletableFuture.completedFuture(8L));
		Mockito.when(asyncFileSystem.read(readHandle2, 8L, 0L))
				.thenReturn(CompletableFuture.completedFuture(Unpooled.wrappedBuffer(new byte[8])));

		ReopenOffsetCommandReader reader = newReader(0);
		Assert.assertNull(reader.read(10));
		Assert.assertEquals(0, sleepCount.get());
		Assert.assertEquals(0L, store.totalLength());

		now.addAndGet(ReadOnlyCommandStore.REOPEN_DEBOUNCE_MILLI);
		ByteBuf buf = reader.read(10);
		Assert.assertNotNull(buf);
		Assert.assertEquals(8, buf.readableBytes());
		Assert.assertEquals(8L, store.totalLength());
		Assert.assertSame(readHandle2, store.getAsyncSegmentFile());
		buf.release();
		reader.close();
	}

	@Test
	public void testZeroByteMissBacksOffWithoutRebuildOrDisconnect() throws Exception {
		Mockito.when(asyncFileSystem.list(readHandle1)).thenReturn(Collections.singletonList(0L));
		Mockito.when(asyncFileSystem.sizeOfSegment(readHandle1, 0L))
				.thenReturn(CompletableFuture.completedFuture(0L));

		Mockito.when(listener.isOpen()).thenAnswer(invocation -> sleepCount.get() < 2);

		store.addCommandsListener(new OffsetReplicationProgress(0), listener);

		Assert.assertEquals(2, sleepCount.get());
		Assert.assertEquals(ReadOnlyCommandStore.MISS_BACKOFF_MILLI, lastSleepMilli.get());
		Mockito.verify(closeableListener, Mockito.never()).close();
		Assert.assertSame(readHandle1, store.getAsyncSegmentFile());
	}

	@Test
	public void testReopenFailThrowsWithoutClosingListener() throws Exception {
		Mockito.when(asyncFileSystem.open(Mockito.anyString(), Mockito.anyString(), Mockito.anyList(),
						Mockito.eq(false), Mockito.anyString()))
				.thenReturn(CompletableFuture.failedFuture(new RuntimeException("open fail")));

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

	@Test
	public void testBackoffSkipsNettyAndCommandHandlerThreads() throws Exception {
		runOnThread("nioEventLoopGroup-2-1", () -> store.missAndBackoff());
		Assert.assertEquals(0, sleepCount.get());

		runOnThread("psync-repl_1", () -> store.missAndBackoff());
		Assert.assertEquals(1, sleepCount.get());
		Assert.assertEquals("psync-repl_1", sleepThreadName.get());
	}

	@Test
	public void testAddCommandsListenerDeliversAfterReopen() throws Exception {
		Mockito.when(asyncFileSystem.list(readHandle1)).thenReturn(Collections.singletonList(0L));
		Mockito.when(asyncFileSystem.sizeOfSegment(readHandle1, 0L))
				.thenReturn(CompletableFuture.completedFuture(0L));
		Mockito.when(asyncFileSystem.list(readHandle2)).thenReturn(Collections.singletonList(0L));
		Mockito.when(asyncFileSystem.sizeOfSegment(readHandle2, 0L))
				.thenReturn(CompletableFuture.completedFuture(4L));
		Mockito.when(asyncFileSystem.read(readHandle2, 4L, 0L))
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

		store.setReopenSleeper(millis -> {
			sleepCount.incrementAndGet();
			lastSleepMilli.set(millis);
			now.addAndGet(ReadOnlyCommandStore.REOPEN_DEBOUNCE_MILLI);
		});

		store.addCommandsListener(new OffsetReplicationProgress(0), listener);

		Assert.assertEquals(4, deliveredBytes.get());
		Mockito.verify(closeableListener, Mockito.never()).close();
		Assert.assertEquals(4L, store.totalLength());
	}

	@Test
	public void testReopenDebounceSkipsCloseOpenWithinWindow() throws Exception {
		Mockito.when(asyncFileSystem.list(readHandle1)).thenReturn(Collections.singletonList(0L));
		Mockito.when(asyncFileSystem.sizeOfSegment(readHandle1, 0L))
				.thenReturn(CompletableFuture.completedFuture(0L));
		Mockito.when(asyncFileSystem.list(readHandle2)).thenReturn(Collections.singletonList(0L));
		Mockito.when(asyncFileSystem.sizeOfSegment(readHandle2, 0L))
				.thenReturn(CompletableFuture.completedFuture(8L));

		store.reopenAndObserve();
		store.reopenAndObserve();
		Assert.assertSame(readHandle1, store.getAsyncSegmentFile());
		Mockito.verify(asyncFileSystem, Mockito.times(2)).open(
				Mockito.anyString(), Mockito.anyString(), Mockito.anyList(), Mockito.eq(false), Mockito.anyString());

		now.addAndGet(ReadOnlyCommandStore.REOPEN_DEBOUNCE_MILLI);
		store.reopenAndObserve();
		Assert.assertSame(readHandle2, store.getAsyncSegmentFile());
		Assert.assertEquals(8L, store.totalLength());
		Mockito.verify(asyncFileSystem, Mockito.times(3)).open(
				Mockito.anyString(), Mockito.anyString(), Mockito.anyList(), Mockito.eq(false), Mockito.anyString());
	}

	@Test
	public void testAddCommandsListenerRethrowsWithoutClosingListener() throws Exception {
		Mockito.when(asyncFileSystem.open(Mockito.anyString(), Mockito.anyString(), Mockito.anyList(),
						Mockito.eq(false), Mockito.anyString()))
				.thenReturn(CompletableFuture.failedFuture(new RuntimeException("open fail")));

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
		Assert.assertFalse(readerText.contains("ReopenSleeper"));
		Assert.assertFalse(readerText.contains("EventMonitor"));
		Assert.assertTrue(storeText.contains("readAt"));
		Assert.assertTrue(storeText.contains("missAndBackoff"));
		Assert.assertTrue(ReadOnlyCommandStore.isForbiddenBackoffThread(
				namedThread("nioEventLoop-1-1")));
		Assert.assertTrue(ReadOnlyCommandStore.isForbiddenBackoffThread(
				namedThread("RedisCommandHandler-0")));
		Assert.assertFalse(ReadOnlyCommandStore.isForbiddenBackoffThread(
				namedThread("psync-repl_1")));
	}

	private ReopenOffsetCommandReader newReader(long offset) {
		return new ReopenOffsetCommandReader(offset, store,
				AbstractCommandStore.DEFAULT_COMMAND_READER_FLYING_THRESHOLD);
	}

	private static Thread namedThread(String name) {
		Thread thread = new Thread();
		thread.setName(name);
		return thread;
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

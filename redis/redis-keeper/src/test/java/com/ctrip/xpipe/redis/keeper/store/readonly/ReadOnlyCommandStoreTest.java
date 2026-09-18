package com.ctrip.xpipe.redis.keeper.store.readonly;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.store.CommandReader;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.CommandsGuarantee;
import com.ctrip.xpipe.redis.core.store.CommandsListener;
import com.ctrip.xpipe.redis.core.store.OffsetReplicationProgress;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.core.store.ratelimit.SyncRateLimiter;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;
import com.ctrip.xpipe.redis.keeper.store.AbstractCommandStore;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(MockitoJUnitRunner.class)
public class ReadOnlyCommandStoreTest {

	private static final File CMD_FILE = new File("/data/repl_1/store_abc/cmd_");

	@Mock
	private AsyncFileSystem asyncFileSystem;

	@Mock
	private AsyncSegmentFile asyncSegmentFile;

	private ReadOnlyCommandStore store;

	@Before
	public void setUp() throws Exception {
		Mockito.lenient().when(asyncFileSystem.open(Mockito.anyString(), Mockito.anyString(), Mockito.anyList(),
						Mockito.eq(false), Mockito.anyString()))
				.thenReturn(CompletableFuture.completedFuture(asyncSegmentFile));
		Mockito.lenient().when(asyncFileSystem.close(asyncSegmentFile))
				.thenReturn(CompletableFuture.completedFuture(null));
		store = new ReadOnlyCommandStore(CMD_FILE, asyncFileSystem, ReplId.from(1L));
	}

	@Test
	public void testInitializeOpensReadOnlyWithoutIndexPrefixes() throws Exception {
		store.initialize();
		store.initialize();

		Mockito.verify(asyncFileSystem, Mockito.times(1)).open(
				Mockito.eq("/data/repl_1/store_abc"),
				Mockito.eq("cmd_"),
				Mockito.eq(Collections.emptyList()),
				Mockito.eq(false),
				Mockito.eq("repl_1"));
		Assert.assertSame(asyncSegmentFile, store.getAsyncSegmentFile());
	}

	@Test
	public void testInitializeSeedsTotalLengthFromLastSegment() throws Exception {
		Mockito.when(asyncFileSystem.list(asyncSegmentFile)).thenReturn(Arrays.asList(100L, 200L));
		Mockito.when(asyncFileSystem.sizeOfSegment(asyncSegmentFile, 200L))
				.thenReturn(CompletableFuture.completedFuture(50L));

		store.initialize();

		Assert.assertEquals(250L, store.totalLength());
	}

	@Test
	public void testInitializeContinuesWhenSeedFails() throws Exception {
		Mockito.when(asyncFileSystem.list(asyncSegmentFile)).thenReturn(Collections.singletonList(0L));
		Mockito.when(asyncFileSystem.sizeOfSegment(asyncSegmentFile, 0L))
				.thenReturn(CompletableFuture.failedFuture(new RuntimeException("size fail")));

		store.initialize();

		Assert.assertSame(asyncSegmentFile, store.getAsyncSegmentFile());
		Assert.assertEquals(0L, store.totalLength());
	}

	@Test
	public void testObserveCurrentEndReopensListingHandleAndRefreshesTotalLength() throws Exception {
		Mockito.when(asyncFileSystem.list(asyncSegmentFile)).thenReturn(Arrays.asList(100L, 200L));
		Mockito.when(asyncFileSystem.sizeOfSegment(asyncSegmentFile, 200L))
				.thenReturn(CompletableFuture.completedFuture(50L));
		store.initialize();
		Assert.assertEquals(250L, store.totalLength());

		Mockito.when(asyncFileSystem.list(asyncSegmentFile)).thenReturn(Arrays.asList(200L, 400L));
		Mockito.when(asyncFileSystem.sizeOfSegment(asyncSegmentFile, 400L))
				.thenReturn(CompletableFuture.completedFuture(80L));

		store.observeCurrentEnd();

		Assert.assertEquals(480L, store.totalLength());
		Mockito.verify(asyncFileSystem, Mockito.times(2)).open(
				Mockito.eq("/data/repl_1/store_abc"),
				Mockito.eq("cmd_"),
				Mockito.eq(Collections.emptyList()),
				Mockito.eq(false),
				Mockito.eq("repl_1"));
		Mockito.verify(asyncFileSystem, Mockito.atLeastOnce()).close(asyncSegmentFile);
	}

	@Test
	public void testTrueImplementations() throws Exception {
		store.initialize();
		Mockito.when(asyncFileSystem.list(asyncSegmentFile)).thenReturn(Arrays.asList(100L, 200L));

		Assert.assertEquals(100L, store.lowestAvailableOffset());
		Mockito.when(asyncFileSystem.list(asyncSegmentFile)).thenReturn(Collections.emptyList());
		Assert.assertEquals(0L, store.lowestAvailableOffset());

		Assert.assertEquals(0L, store.totalLength());
		store.refreshTotalLength(50L);
		Assert.assertEquals(50L, store.totalLength());

		Assert.assertEquals(Long.MAX_VALUE, store.lowestReadingOffset());
		CommandReader<?> reader1 = Mockito.mock(CommandReader.class);
		CommandReader<?> reader2 = Mockito.mock(CommandReader.class);
		Mockito.when(reader1.getReadOffset()).thenReturn(80L);
		Mockito.when(reader2.getReadOffset()).thenReturn(40L);
		store.addReader(reader1);
		store.addReader(reader2);
		Assert.assertEquals(40L, store.lowestReadingOffset());
		store.removeReader(reader2);
		Assert.assertEquals(80L, store.lowestReadingOffset());

		Assert.assertEquals("data.repl_1", store.simpleDesc());
		store.makeSureOpen();

		Assert.assertEquals(0L, store.getCommandsLastUpdatedAt());

		store.close();
		try {
			store.makeSureOpen();
			Assert.fail("expected closed");
		} catch (IllegalStateException expected) {
			Assert.assertTrue(expected.getMessage().contains("closed"));
		}
	}

	@Test
	public void testEmptyImplementationsDoNotThrow() throws Exception {
		store.flushSlidingWindow();
		store.flushPendingData();
		store.attachRateLimiter(SyncRateLimiter.UNLIMITED);
	}

	@Test
	public void testUnsupportedMethods() throws Exception {
		GtidSet empty = new GtidSet(GtidSet.EMPTY_GTIDSET);
		assertUnsupported("appendCommands", () -> store.appendCommands(Unpooled.EMPTY_BUFFER));
		assertUnsupported("onlyAppendCommand", () -> store.onlyAppendCommand(Unpooled.EMPTY_BUFFER));
		assertUnsupported("awaitCommandsOffset", () -> store.awaitCommandsOffset(0, 1));
		assertUnsupported("gc", store::gc);
		assertUnsupported("rotateFileIfNecessary", store::rotateFileIfNecessary);
		assertUnsupported("destroy", store::destroy);
		assertUnsupported("switchToXSync", () -> store.switchToXSync(empty));
		assertUnsupported("switchToPsync", () -> store.switchToPsync("?", 0));
		assertUnsupported("restoreXsyncIndex", store::restoreXsyncIndex);
		assertUnsupported("rebindIndexWritersIfUnbound", store::rebindIndexWritersIfUnbound);
		assertUnsupported("locateContinueGtidSet", () -> store.locateContinueGtidSet(empty));
		assertUnsupported("locateContinueGtidSetWithFallbackToEnd",
				() -> store.locateContinueGtidSetWithFallbackToEnd(empty));
		assertUnsupported("locateTailOfCmd", store::locateTailOfCmd);
		assertUnsupported("getIndexGtidSet", store::getIndexGtidSet);
		assertUnsupported("locateCmdSegment", () -> store.locateCmdSegment("uuid", 0, 1));
		assertUnsupported("retainCommands", () -> store.retainCommands(Mockito.mock(CommandsGuarantee.class)));
		assertUnsupported("increaseLostNotInCmdStore",
				() -> store.increaseLostNotInCmdStore(empty, () -> true));
		assertUnsupported("resetStateForContinue", store::resetStateForContinue);
	}

	@Test
	public void testAll32MethodsCovered() throws Exception {
		AtomicInteger covered = new AtomicInteger();
		store.initialize();
		covered.incrementAndGet(); // initialize
		store.totalLength();
		covered.incrementAndGet();
		Mockito.when(asyncFileSystem.list(asyncSegmentFile)).thenReturn(Collections.singletonList(0L));
		store.lowestAvailableOffset();
		covered.incrementAndGet();
		store.addReader(Mockito.mock(CommandReader.class));
		covered.incrementAndGet();
		store.lowestReadingOffset();
		covered.incrementAndGet();
		store.removeReader(Mockito.mock(CommandReader.class));
		covered.incrementAndGet();
		store.simpleDesc();
		covered.incrementAndGet();
		store.makeSureOpen();
		covered.incrementAndGet();
		store.getCommandsLastUpdatedAt();
		covered.incrementAndGet();
		store.flushSlidingWindow();
		covered.incrementAndGet();
		store.flushPendingData();
		covered.incrementAndGet();
		store.attachRateLimiter(SyncRateLimiter.UNLIMITED);
		covered.incrementAndGet();
		invokeAllUnsupported(store);
		covered.addAndGet(18);
		CommandsListener idleListener = Mockito.mock(CommandsListener.class);
		Mockito.when(idleListener.isOpen()).thenReturn(false);
		store.addCommandsListener(new OffsetReplicationProgress(0), idleListener);
		covered.incrementAndGet();
		store.close();
		covered.incrementAndGet();
		Assert.assertEquals(32, covered.get());
	}

	@Test
	public void testTotalLengthSnapshotMonotonic() {
		Assert.assertEquals(0L, store.totalLength());
		store.refreshTotalLength(100L);
		Assert.assertEquals(100L, store.totalLength());
		store.refreshTotalLength(80L);
		Assert.assertEquals(100L, store.totalLength());
		store.refreshTotalLength(-1L);
		Assert.assertEquals(100L, store.totalLength());
		store.refreshTotalLength(150L);
		Assert.assertEquals(150L, store.totalLength());
	}

	@Test
	public void testDoesNotDependOnWriteSideMachinery() throws Exception {
		Assert.assertFalse(AbstractCommandStore.class.isAssignableFrom(ReadOnlyCommandStore.class));
		Assert.assertTrue(CommandStore.class.isAssignableFrom(ReadOnlyCommandStore.class));

		File source = new File("src/main/java/com/ctrip/xpipe/redis/keeper/store/readonly/ReadOnlyCommandStore.java");
		Assert.assertTrue("source missing: " + source.getAbsolutePath(), source.isFile());
		String text = stripComments(new String(Files.readAllBytes(source.toPath()), StandardCharsets.UTF_8));
		for (String forbidden : Arrays.asList("cmdWriter", "OffsetNotifier", "TimerSlidingWindow", "IndexStore")) {
			Assert.assertFalse("ReadOnlyCommandStore must not reference " + forbidden, text.contains(forbidden));
		}
		Assert.assertFalse(text.contains("extends AbstractCommandStore"));
	}

	private static void invokeAllUnsupported(ReadOnlyCommandStore store) throws Exception {
		GtidSet empty = new GtidSet(GtidSet.EMPTY_GTIDSET);
		assertUnsupported("appendCommands", () -> store.appendCommands(Unpooled.EMPTY_BUFFER));
		assertUnsupported("onlyAppendCommand", () -> store.onlyAppendCommand(Unpooled.EMPTY_BUFFER));
		assertUnsupported("awaitCommandsOffset", () -> store.awaitCommandsOffset(0, 1));
		assertUnsupported("gc", store::gc);
		assertUnsupported("rotateFileIfNecessary", store::rotateFileIfNecessary);
		assertUnsupported("destroy", store::destroy);
		assertUnsupported("switchToXSync", () -> store.switchToXSync(empty));
		assertUnsupported("switchToPsync", () -> store.switchToPsync("?", 0));
		assertUnsupported("restoreXsyncIndex", store::restoreXsyncIndex);
		assertUnsupported("rebindIndexWritersIfUnbound", store::rebindIndexWritersIfUnbound);
		assertUnsupported("locateContinueGtidSet", () -> store.locateContinueGtidSet(empty));
		assertUnsupported("locateContinueGtidSetWithFallbackToEnd",
				() -> store.locateContinueGtidSetWithFallbackToEnd(empty));
		assertUnsupported("locateTailOfCmd", store::locateTailOfCmd);
		assertUnsupported("getIndexGtidSet", store::getIndexGtidSet);
		assertUnsupported("locateCmdSegment", () -> store.locateCmdSegment("uuid", 0, 1));
		assertUnsupported("retainCommands", () -> store.retainCommands(Mockito.mock(CommandsGuarantee.class)));
		assertUnsupported("increaseLostNotInCmdStore",
				() -> store.increaseLostNotInCmdStore(empty, () -> true));
		assertUnsupported("resetStateForContinue", store::resetStateForContinue);
	}

	private static void assertUnsupported(String method, ThrowingRunnable action) throws Exception {
		try {
			action.run();
			Assert.fail("expected UnsupportedOperationException for " + method);
		} catch (UnsupportedOperationException e) {
			Assert.assertTrue("message should contain class name: " + e.getMessage(),
					e.getMessage().contains("ReadOnlyCommandStore"));
			Assert.assertTrue("message should contain method " + method + ": " + e.getMessage(),
					e.getMessage().contains(method));
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

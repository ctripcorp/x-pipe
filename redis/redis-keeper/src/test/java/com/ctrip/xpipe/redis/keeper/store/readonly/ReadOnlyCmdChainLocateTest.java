package com.ctrip.xpipe.redis.keeper.store.readonly;

import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;
import com.ctrip.xpipe.redis.keeper.store.AbstractCommandStore;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Phase OS (T-OS.5 ①④⑤⑥⑦⑩⑪)：段不连续的不变式校验与快照区间校验。AC-6b ②③④⑤⑦。
 * <p>
 * 只覆盖 Store / Reader 侧语义（{@code openAndObserve()} 的三态、快照重建、Reader 中断判定）；
 * Watcher 的 30s 二分节奏在 {@code PrepareStoreWatcherLocateMissTest}。
 */
@RunWith(MockitoJUnitRunner.class)
public class ReadOnlyCmdChainLocateTest {

	private static final File CMD_FILE = new File("/data/repl_1/store_abc/cmd_");

	/**
	 * D49 明确撤销的旁路变量：区间校验就是唯一的中断信号，不得再引入 generation / lineage / dirty flag。
	 * 名字按片段拼接，避免本文件自身命中门禁。
	 */
	private static final List<String> FORBIDDEN_SIDE_CHANNELS =
			Arrays.asList("gener" + "ation", "line" + "age", "dirty" + "Flag");

	/**
	 * v1.79 撤销的两个打点：段不连续在两相节奏下是常态，观测靠 WARN / ERROR 日志。
	 * 同样按片段拼接，避免本文件自身命中门禁。
	 */
	private static final List<String> WITHDRAWN_METRICS =
			Arrays.asList("prepareReopen" + "LocateMiss", "prepareCmd" + "ChainRebuilt");

	@Mock
	private AsyncFileSystem asyncFileSystem;

	@Mock
	private AsyncSegmentFile handle;

	private ReadOnlyCommandStore store;

	@Before
	public void setUp() throws Exception {
		Mockito.lenient().when(asyncFileSystem.open(Mockito.anyString(), Mockito.anyString(), Mockito.anyList(),
						Mockito.eq(false), Mockito.anyString()))
				.thenReturn(CompletableFuture.completedFuture(handle));
		Mockito.lenient().when(asyncFileSystem.close(handle))
				.thenReturn(CompletableFuture.completedFuture(null));
		store = new ReadOnlyCommandStore(CMD_FILE, asyncFileSystem, ReplId.from(1L));
	}

	/**
	 * AC-6b ②：FS-M5.6 场景 —— 新段可见、老段 size 滞后（{@code segs[0] > observedEnd}）⇒ {@code LOCATE_MISS}；
	 * 快照整份保留（{@code lowestAvailableOffset} 不跟着 {@code firstOffset} 跳高）、WARN + 计数、
	 * **Reader 未被中断**。
	 */
	@Test
	public void testNewSegmentVisibleWhileOldSegmentLagsReportsLocateMiss() throws Exception {
		seedChain(new long[]{0L}, 100L);
		Assert.assertEquals(100L, store.totalLength());
		Assert.assertEquals(0L, store.lowestAvailableOffset());

		// 老段掉链：只剩下新段 [200, 250)
		stubChain(new long[]{200L}, 50L);
		store.closeHandleForCycle();

		Assert.assertEquals(ReadOnlyCommandStore.ObserveResult.LOCATE_MISS, store.openAndObserve());

		Assert.assertEquals(1L, store.getLocateMissCount());
		Assert.assertEquals(100L, store.totalLength());
		Assert.assertEquals(0L, store.lowestAvailableOffset());

		// Reader 仍落在旧快照区间内：没有新数据可读，但不被中断
		stubRead(50L, 50L, 8);
		ReopenOffsetCommandReader reader = newReader(50L);
		ByteBuf buf = reader.read(10);
		Assert.assertNotNull(buf);
		Assert.assertEquals(8, buf.readableBytes());
		buf.release();
		reader.close();
	}

	/**
	 * AC-6b ③：老段 size 补齐后下一个开相自动恢复 {@code OPENED} 并继续推进。
	 */
	@Test
	public void testChainRecoversToOpenedAfterOldSegmentSizeCatchesUp() throws Exception {
		seedChain(new long[]{0L}, 100L);

		stubChain(new long[]{200L}, 50L);
		store.closeHandleForCycle();
		Assert.assertEquals(ReadOnlyCommandStore.ObserveResult.LOCATE_MISS, store.openAndObserve());
		Assert.assertEquals(100L, store.totalLength());

		// 老段 size 补齐，链条重新连续
		stubChain(new long[]{0L, 200L}, 50L);
		store.closeHandleForCycle();
		Assert.assertEquals(ReadOnlyCommandStore.ObserveResult.OPENED, store.openAndObserve());

		Assert.assertEquals(250L, store.totalLength());
		Assert.assertEquals(0L, store.lowestAvailableOffset());
		Assert.assertEquals(1L, store.getLocateMissCount());
	}

	/**
	 * AC-6b ④⑤：重建后 Reader 落在**空洞里**（{@code < firstOffset}）抛 {@code IOException}。
	 */
	@Test
	public void testRebuiltSnapshotBreaksReaderInsideHole() throws Exception {
		seedChain(new long[]{0L}, 100L);
		ReopenOffsetCommandReader reader = newReader(50L);

		stubChain(new long[]{200L}, 50L);
		Assert.assertTrue(store.rebuildSnapshotFromCurrentChain());
		Assert.assertEquals(200L, store.lowestAvailableOffset());
		Assert.assertEquals(250L, store.totalLength());

		assertReadThrows(reader, "curPosition=50");
		reader.close();
	}

	/**
	 * AC-6b ④⑤⑥：占槽方原地重置 —— 快照重建让 {@code observedEnd} **真的回退**（绕过 max guard），
	 * 落在新尾右边的 Reader 抛 {@code IOException}（补掉「只看 {@code visible <= 0} 会静默永久停住」）。
	 */
	@Test
	public void testRebuiltSnapshotAllowsObservedEndRegressionAndBreaksReaderPastTail() throws Exception {
		seedChain(new long[]{0L}, 100L);
		ReopenOffsetCommandReader reader = newReader(50L);

		// 原地重置：同一个段起点，长度回退到 30
		stubChain(new long[]{0L}, 30L);
		Assert.assertTrue(store.rebuildSnapshotFromCurrentChain());
		Assert.assertEquals("observedEnd must be allowed to regress", 30L, store.totalLength());
		Assert.assertEquals(0L, store.lowestAvailableOffset());

		assertReadThrows(reader, "observedEnd=30");
		reader.close();

		// 对比：同一份链条走 refreshTotalLength 时 max guard 仍生效（防御性语义未被改掉）
		store.refreshTotalLength(10L);
		Assert.assertEquals(30L, store.totalLength());
	}

	/**
	 * AC-6b ⑤：正常追尾（{@code curPosition == observedEnd}）不得被误判；重建后仍落在
	 * {@code [firstOffset, observedEnd]} 内的 Reader **不**被中断 —— 链条恒连续，继续读是安全的。
	 */
	@Test
	public void testTailFollowingAndInRangeReaderAfterRebuildAreNotBroken() throws Exception {
		seedChain(new long[]{0L}, 100L);

		// 正好追上可见尾：只 return null，不抛
		ReopenOffsetCommandReader atTail = newReader(100L);
		Assert.assertNull(atTail.read(10));
		atTail.close();

		ReopenOffsetCommandReader inRange = newReader(50L);
		stubChain(new long[]{0L, 200L}, 50L);
		Assert.assertTrue(store.rebuildSnapshotFromCurrentChain());
		Assert.assertEquals(0L, store.lowestAvailableOffset());
		Assert.assertEquals(250L, store.totalLength());

		stubRead(50L, 200L, 16);
		ByteBuf buf = inRange.read(10);
		Assert.assertNotNull(buf);
		Assert.assertEquals(16, buf.readableBytes());
		buf.release();
		inRange.close();
	}

	/**
	 * AC-6b ⑤ / D49：占槽方 GC 抬高 {@code firstOffset} 时，同一条校验只断越界的那个 Reader。
	 */
	@Test
	public void testGcRaisingFirstOffsetBreaksOnlyTheOutOfRangeReader() throws Exception {
		seedChain(new long[]{0L, 100L}, 100L);
		Assert.assertEquals(200L, store.totalLength());

		ReopenOffsetCommandReader behind = newReader(50L);
		ReopenOffsetCommandReader ahead = newReader(150L);

		// GC 删掉前缀，链条仍覆盖上次观察末尾 ⇒ OPENED
		stubChain(new long[]{100L}, 100L);
		store.closeHandleForCycle();
		Assert.assertEquals(ReadOnlyCommandStore.ObserveResult.OPENED, store.openAndObserve());
		Assert.assertEquals(100L, store.lowestAvailableOffset());
		Assert.assertEquals(200L, store.totalLength());

		assertReadThrows(behind, "firstOffset=100");

		stubRead(150L, 50L, 4);
		ByteBuf buf = ahead.read(10);
		Assert.assertNotNull(buf);
		Assert.assertEquals(4, buf.readableBytes());
		buf.release();

		behind.close();
		ahead.close();
	}

	/**
	 * AC-6b ⑧：「从未成功观察过」与「观察到空目录」严格区分 —— `initialize()` 种子失败后快照仍是
	 * `EMPTY` 哨兵，此时**没有 probe**，首次观察即便 `segs[0] > 0`（占槽方已 GC 过前缀）也算 `OPENED`，
	 * 不白等一个 30s 宽限。
	 */
	@Test
	public void testFirstObservationAfterSeedFailureIsAcceptedWithoutProbe() throws Exception {
		// 种子观察失败：list 正常但末段 size 抛异常，initialize 只 WARN
		Mockito.when(asyncFileSystem.list(handle)).thenReturn(Collections.singletonList(1000L));
		Mockito.when(asyncFileSystem.sizeOfSegment(handle, 1000L))
				.thenReturn(CompletableFuture.failedFuture(new RuntimeException("size fail")),
						CompletableFuture.completedFuture(200L));
		store.initialize();
		Assert.assertSame("seed failure must keep the never-observed sentinel",
				ReadOnlyCmdOffsetSnapshot.EMPTY, store.offsetSnapshot());

		store.closeHandleForCycle();
		Assert.assertEquals(ReadOnlyCommandStore.ObserveResult.OPENED, store.openAndObserve());

		Assert.assertEquals(0L, store.getLocateMissCount());
		Assert.assertEquals(1000L, store.lowestAvailableOffset());
		Assert.assertEquals(1200L, store.totalLength());
	}

	/**
	 * AC-6b ⑧ 的另一半：**观察到过**空目录之后再出现 `segs[0] > 0` 的链条，是真不连续，照判
	 * `LOCATE_MISS` —— 观察到的空目录必须是新实例，不能复用 `EMPTY` 常量。
	 */
	@Test
	public void testObservedEmptyDirIsNotTheNeverObservedSentinel() throws Exception {
		Mockito.when(asyncFileSystem.list(handle)).thenReturn(Collections.emptyList());
		store.initialize();
		Assert.assertNotSame("observed empty dir must not reuse the sentinel",
				ReadOnlyCmdOffsetSnapshot.EMPTY, store.offsetSnapshot());
		Assert.assertEquals(0L, store.totalLength());

		stubChain(new long[]{1000L}, 200L);
		store.closeHandleForCycle();
		Assert.assertEquals(ReadOnlyCommandStore.ObserveResult.LOCATE_MISS, store.openAndObserve());
		Assert.assertEquals(1L, store.getLocateMissCount());
		Assert.assertEquals(0L, store.totalLength());
	}

	/**
	 * AC-6b ⑦：空目录两种 {@code probe} 分支 —— {@code observedEnd == 0} 算 {@code OPENED}，
	 * {@code observedEnd > 0} 走 {@code LOCATE_MISS}。
	 */
	@Test
	public void testEmptyDirTakesBothProbeBranches() throws Exception {
		Mockito.when(asyncFileSystem.list(handle)).thenReturn(Collections.emptyList());
		store.initialize();
		Assert.assertEquals(0L, store.totalLength());

		store.closeHandleForCycle();
		Assert.assertEquals(ReadOnlyCommandStore.ObserveResult.OPENED, store.openAndObserve());
		Assert.assertEquals(0L, store.getLocateMissCount());

		// 观察到过数据之后目录又变空：probe > 0，按定位不到处置
		stubChain(new long[]{0L}, 100L);
		store.closeHandleForCycle();
		Assert.assertEquals(ReadOnlyCommandStore.ObserveResult.OPENED, store.openAndObserve());
		Assert.assertEquals(100L, store.totalLength());

		Mockito.when(asyncFileSystem.list(handle)).thenReturn(Collections.emptyList());
		store.closeHandleForCycle();
		Assert.assertEquals(ReadOnlyCommandStore.ObserveResult.LOCATE_MISS, store.openAndObserve());
		Assert.assertEquals(100L, store.totalLength());
		Assert.assertEquals(1L, store.getLocateMissCount());
	}

	/**
	 * T-OS.5 ⑪ 源码门禁：{@code readonly} 包内无 generation / lineage 类旁路变量；
	 * {@code doRead} 只读一次快照引用（Reader 源码里 {@code offsetSnapshot()} 恰好出现一次）。
	 * AC-6b ⑨：两个打点已撤销，`readonly` 包与 Watcher 内不得再出现。
	 */
	@Test
	public void testNoSideChannelVariablesAndSingleSnapshotRead() throws Exception {
		Path packageDir = new File("src/main/java/com/ctrip/xpipe/redis/keeper/store/readonly").toPath();
		Assert.assertTrue("readonly package missing: " + packageDir, Files.isDirectory(packageDir));
		List<Path> sources;
		try (Stream<Path> paths = Files.walk(packageDir)) {
			sources = paths.filter(Files::isRegularFile)
					.filter(path -> path.toString().endsWith(".java"))
					.collect(Collectors.toCollection(ArrayList::new));
		}
		Assert.assertTrue("readonly package sources not scanned: " + sources, sources.size() >= 3);
		List<String> hits = new ArrayList<>();
		for (Path path : sources) {
			String text = stripComments(new String(Files.readAllBytes(path), StandardCharsets.UTF_8));
			for (String forbidden : FORBIDDEN_SIDE_CHANNELS) {
				if (text.contains(forbidden)) {
					hits.add(forbidden + " @ " + path);
				}
			}
		}
		Assert.assertTrue("D49 forbids side-channel variables: " + hits, hits.isEmpty());

		// 打点已撤销（v1.79）：观测只靠 WARN / ERROR 日志
		sources.add(new File("src/main/java/com/ctrip/xpipe/redis/keeper/prepare/PrepareStoreWatcher.java").toPath());
		for (Path path : sources) {
			String text = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
			for (String metric : WITHDRAWN_METRICS) {
				Assert.assertFalse(metric + " is withdrawn @ " + path, text.contains(metric));
			}
		}

		String readerText = stripComments(new String(Files.readAllBytes(
				packageDir.resolve("ReopenOffsetCommandReader.java")), StandardCharsets.UTF_8));
		Assert.assertEquals("doRead must read the volatile snapshot exactly once",
				1, countOccurrences(readerText, "offsetSnapshot()"));
		Assert.assertFalse("visible must come from the same local snapshot reference",
				readerText.contains("commandStore.totalLength()"));
	}

	private void seedChain(long[] segmentStarts, long lastSegmentSize) throws Exception {
		stubChain(segmentStarts, lastSegmentSize);
		store.initialize();
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

	private void stubRead(long offset, long length, int returnedBytes) {
		Mockito.when(asyncFileSystem.read(handle, length, offset))
				.thenReturn(CompletableFuture.completedFuture(Unpooled.wrappedBuffer(new byte[returnedBytes])));
	}

	private ReopenOffsetCommandReader newReader(long offset) {
		return new ReopenOffsetCommandReader(offset, store,
				AbstractCommandStore.DEFAULT_COMMAND_READER_FLYING_THRESHOLD);
	}

	private static void assertReadThrows(ReopenOffsetCommandReader reader, String expectedInMessage) {
		try {
			reader.read(10);
			Assert.fail("expected IOException for " + expectedInMessage);
		} catch (IOException expected) {
			Assert.assertTrue("message should contain " + expectedInMessage + ", got " + expected.getMessage(),
					expected.getMessage().contains(expectedInMessage));
		}
	}

	private static int countOccurrences(String text, String token) {
		int count = 0;
		int idx = text.indexOf(token);
		while (idx >= 0) {
			count++;
			idx = text.indexOf(token, idx + token.length());
		}
		return count;
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
}

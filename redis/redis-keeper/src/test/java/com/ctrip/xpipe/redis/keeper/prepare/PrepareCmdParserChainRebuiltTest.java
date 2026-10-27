package com.ctrip.xpipe.redis.keeper.prepare;

import com.ctrip.xpipe.redis.core.store.BacklogOffsetReplicationProgress;
import com.ctrip.xpipe.redis.core.store.CommandsListener;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.core.store.ReplicationProgress;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import com.ctrip.xpipe.redis.keeper.store.readonly.ReadOnlyCommandStore;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Phase OS (T-OS.5 ⑧)：快照重建中断 Reader 后，{@code PrepareCmdParser} 走既有恢复路径 ——
 * {@code runLoop} catch → {@code hook.reset()} → 从新的 {@code backlogEndOffset()} 重挂并继续消费。AC-6b ⑤。
 * <p>
 * 用 mock FS 构造链条，才能把「老段掉链、原 offset 落进空洞」这种 TFS 时序精确复现。
 */
public class PrepareCmdParserChainRebuiltTest extends AbstractRedisKeeperTest {

	private static final File CMD_FILE = new File("/data/repl_1/store_abc/cmd_");

	/**
	 * 重建后的新段起点：远在原 offset 右边，使原 Reader 必然落进空洞。
	 */
	private static final long REBUILT_SEGMENT_START = 4096L;

	private AsyncFileSystem asyncFileSystem;

	private AsyncSegmentFile handle;

	private ReadOnlyCommandStore cmdStore;

	private ReplicationStoreManager manager;

	private final List<String> received = new CopyOnWriteArrayList<>();

	@Before
	public void beforePrepareCmdParserChainRebuiltTest() throws Exception {
		received.clear();
		asyncFileSystem = Mockito.mock(AsyncFileSystem.class);
		handle = Mockito.mock(AsyncSegmentFile.class);
		Mockito.when(asyncFileSystem.open(Mockito.anyString(), Mockito.anyString(), Mockito.anyList(),
						Mockito.eq(false), Mockito.anyString()))
				.thenReturn(CompletableFuture.completedFuture(handle));
		Mockito.when(asyncFileSystem.close(handle)).thenReturn(CompletableFuture.completedFuture(null));

		cmdStore = new ReadOnlyCommandStore(CMD_FILE, asyncFileSystem, ReplId.from(1L));
		stubChain(new long[]{0L}, 0L);
		cmdStore.initialize();

		DefaultReplicationStore store = Mockito.mock(DefaultReplicationStore.class);
		Mockito.when(store.getCommandStore()).thenReturn(cmdStore);
		Mockito.when(store.backlogEndOffset()).thenAnswer(in -> cmdStore.totalLength());
		Mockito.doAnswer(in -> {
			ReplicationProgress<?> progress = in.getArgument(0);
			CommandsListener listener = in.getArgument(1);
			cmdStore.addCommandsListener(progress, listener);
			return null;
		}).when(store).addCommandsListener(Mockito.any(), Mockito.any());

		manager = Mockito.mock(ReplicationStoreManager.class);
		Mockito.when(manager.getReplId()).thenReturn(ReplId.from(1L));
		Mockito.when(manager.isReadOnly()).thenReturn(true);
		Mockito.when(manager.getCurrent()).thenReturn(store);
	}

	@Test
	public void testParserReattachesFromNewBacklogEndAfterSnapshotRebuild() throws Exception {
		byte[] beforeRebuild = publishBytes("delay", "t1");
		byte[] afterRebuild = publishBytes("delay", "t2");

		PrepareCmdParser parser = new PrepareCmdParser(manager, createRedisOpParser(),
				(channel, message) -> received.add(channel + ":" + message));
		try {
			parser.start();
			waitConditionUntilTimeOut(() -> parser.getAttachCount() >= 1);

			// 第一段数据可见：Parser 在原 offset 上正常消费
			stubChain(new long[]{0L}, beforeRebuild.length);
			stubRead(0L, beforeRebuild.length, beforeRebuild);
			cmdStore.closeHandleForCycle();
			Assert.assertEquals(ReadOnlyCommandStore.ObserveResult.OPENED, cmdStore.openAndObserve());
			waitConditionUntilTimeOut(() -> received.contains("delay:t1"));

			// 老段掉链：按新链条重建快照，原 offset 落进空洞 ⇒ Reader 抛 IOException 断开
			long attachBefore = parser.getAttachCount();
			stubChain(new long[]{REBUILT_SEGMENT_START}, afterRebuild.length);
			Assert.assertTrue(cmdStore.rebuildSnapshotFromCurrentChain());
			Assert.assertEquals(REBUILT_SEGMENT_START, cmdStore.lowestAvailableOffset());

			// 既有机制恢复：runLoop catch → hook.reset() → 从新的 backlogEndOffset() 重挂
			waitConditionUntilTimeOut(() -> parser.getAttachCount() > attachBefore);

			// 重挂点之后的新增数据继续被消费
			long reattachOffset = REBUILT_SEGMENT_START + afterRebuild.length;
			stubChain(new long[]{REBUILT_SEGMENT_START}, afterRebuild.length * 2L);
			stubRead(reattachOffset, afterRebuild.length, afterRebuild);
			cmdStore.closeHandleForCycle();
			Assert.assertEquals(ReadOnlyCommandStore.ObserveResult.OPENED, cmdStore.openAndObserve());

			waitConditionUntilTimeOut(() -> received.contains("delay:t2"));
			Assert.assertTrue(received.contains("delay:t1"));
		} finally {
			parser.stop();
		}
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

	private void stubRead(long offset, long length, byte[] data) {
		Mockito.when(asyncFileSystem.read(handle, length, offset))
				.thenAnswer(in -> CompletableFuture.completedFuture(Unpooled.wrappedBuffer(data)));
	}

	private static byte[] publishBytes(String channel, String message) {
		String body = "*3\r\n$7\r\nPUBLISH\r\n$" + channel.length() + "\r\n" + channel
				+ "\r\n$" + message.length() + "\r\n" + message + "\r\n";
		return body.getBytes(StandardCharsets.US_ASCII);
	}
}

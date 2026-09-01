package com.ctrip.xpipe.redis.keeper.store.readonly;

import com.ctrip.xpipe.api.utils.IOSupplier;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.store.BacklogOffsetReplicationProgress;
import com.ctrip.xpipe.redis.core.store.CommandReader;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.CommandsGuarantee;
import com.ctrip.xpipe.redis.core.store.CommandsListener;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.core.store.ReplicationProgress;
import com.ctrip.xpipe.redis.core.store.ratelimit.SyncRateLimiter;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;
import com.ctrip.xpipe.redis.keeper.store.AbstractStore;
import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * PREPARE 只读 CommandStore（m5 D5b / D30 / D30b）。
 * {@code implements CommandStore}，不继承生产写路径实现。
 */
public class ReadOnlyCommandStore extends AbstractStore implements CommandStore {

	private static final Logger logger = LoggerFactory.getLogger(ReadOnlyCommandStore.class);

	private static final List<String> NO_INDEX_PREFIXES = Collections.emptyList();

	private final File baseDir;

	private final String fileNamePrefix;

	private final AsyncFileSystem asyncFileSystem;

	private final ReplId fileSystemReplId;

	private final AtomicBoolean initialized = new AtomicBoolean(false);

	private final ConcurrentMap<CommandReader<?>, Boolean> readers = new ConcurrentHashMap<>();

	private final AtomicLong totalLengthSnapshot = new AtomicLong(0L);

	private volatile long commandsLastUpdatedAt;

	private volatile AsyncSegmentFile asyncSegmentFile;

	public ReadOnlyCommandStore(File cmdFile, AsyncFileSystem asyncFileSystem, ReplId fileSystemReplId) {
		this.baseDir = Objects.requireNonNull(cmdFile, "cmdFile").getParentFile();
		this.fileNamePrefix = cmdFile.getName();
		this.asyncFileSystem = Objects.requireNonNull(asyncFileSystem, "asyncFileSystem");
		this.fileSystemReplId = Objects.requireNonNull(fileSystemReplId, "fileSystemReplId");
	}

	@Override
	public void initialize() throws IOException {
		if (!initialized.compareAndSet(false, true)) {
			return;
		}
		this.asyncSegmentFile = AsyncFileSystemHelper.awaitOpen(asyncFileSystem,
				() -> asyncFileSystem.open(baseDir.getAbsolutePath(), fileNamePrefix, NO_INDEX_PREFIXES, false,
						fileSystemReplId.toString()),
				"open read-only command segment " + fileNamePrefix);
		seedTotalLengthFromOpenHandle();
		logger.info("[initialize]{}", this);
	}

	@Override
	public void makeSureOpen() {
		super.makeSureOpen();
		if (!initialized.get()) {
			throw new IllegalStateException("[makeSureOpen][uninitialized]" + this);
		}
	}

	@Override
	public void close() {
		if (!cmpAndSetClosed()) {
			logger.warn("[close][already closed]{}", this);
			return;
		}
		logger.info("[close]{}", this);
		AsyncFileSystemHelper.closeHandle(asyncFileSystem, asyncSegmentFile, "close read-only command segment " + fileNamePrefix);
	}

	@Override
	public long totalLength() {
		return totalLengthSnapshot.get();
	}

	/**
	 * 观察快照只前进（D30）。{@code observedEnd} 为当前段 {@code startOffset + size}。
	 * <p>
	 * 包内调用方：{@code initialize()} / {@code observeCurrentEnd()} 观察 listing 句柄；
	 * {@code ReopenOffsetCommandReader} 每次 reopen 观察到段 size 后（含读到 0 字节、分支② 仍能 list 到当前段）。
	 */
	void refreshTotalLength(long observedEnd) {
		if (observedEnd < 0) {
			return;
		}
		totalLengthSnapshot.accumulateAndGet(observedEnd, Math::max);
	}

	/**
	 * 对外更新接口（D30）：reopen Store 自有 listing 句柄并刷新 {@code totalLength} 快照。
	 * 句柄只由本类及其持有的 reader 访问；Watcher 无 reader 时调本方法，禁止直接操作句柄。
	 * 调用线程：Watcher / initialize 所在线程；禁止 Redis 命令线程（D11）。
	 * Store 不自建定时器。失败只 WARN，快照保持原值。
	 */
	public void observeCurrentEnd() {
		makeSureOpen();
		try {
			reopenListingHandle();
			observeSizeOnCurrentHandle();
		} catch (Throwable th) {
			logger.warn("[observeCurrentEnd][fail]{}", this, th);
		}
	}

	private void seedTotalLengthFromOpenHandle() {
		observeSizeOnCurrentHandle();
	}

	private void reopenListingHandle() throws IOException {
		AsyncSegmentFile previous = asyncSegmentFile;
		AsyncFileSystemHelper.closeHandle(asyncFileSystem, previous,
				"reopen read-only command listing handle " + fileNamePrefix);
		this.asyncSegmentFile = AsyncFileSystemHelper.awaitOpen(asyncFileSystem,
				() -> asyncFileSystem.open(baseDir.getAbsolutePath(), fileNamePrefix, NO_INDEX_PREFIXES, false,
						fileSystemReplId.toString()),
				"reopen read-only command segment " + fileNamePrefix);
	}

	private void observeSizeOnCurrentHandle() {
		try {
			List<Long> segmentOffsets = asyncFileSystem.list(asyncSegmentFile);
			if (segmentOffsets == null || segmentOffsets.isEmpty()) {
				return;
			}
			long lastStart = segmentOffsets.get(segmentOffsets.size() - 1);
			Long segmentSize = AsyncFileSystemHelper.await(
					() -> asyncFileSystem.sizeOfSegment(asyncSegmentFile, lastStart),
					"size read-only command segment " + fileNamePrefix);
			if (segmentSize != null && segmentSize >= 0) {
				refreshTotalLength(lastStart + segmentSize);
			}
		} catch (Throwable th) {
			logger.warn("[observeSize][fail]{}", this, th);
		}
	}

	@Override
	public long lowestAvailableOffset() {
		makeSureOpen();
		List<Long> segmentOffsets = asyncFileSystem.list(asyncSegmentFile);
		if (segmentOffsets == null || segmentOffsets.isEmpty()) {
			logger.info("[lowestAvailableOffset][no cmd segments][start offset 0]");
			return 0L;
		}
		return segmentOffsets.get(0);
	}

	@Override
	public void addReader(CommandReader<?> reader) {
		this.readers.put(reader, Boolean.TRUE);
	}

	@Override
	public void removeReader(CommandReader<?> reader) {
		this.readers.remove(reader);
	}

	@Override
	public long lowestReadingOffset() {
		long lowest = Long.MAX_VALUE;
		for (CommandReader<?> reader : readers.keySet()) {
			long readingOffset = reader.getReadOffset();
			if (readingOffset >= 0) {
				lowest = Math.min(lowest, readingOffset);
			}
		}
		return lowest;
	}

	@Override
	public String simpleDesc() {
		File desc1 = baseDir == null ? null : baseDir.getParentFile();
		File desc2 = desc1 == null ? null : desc1.getParentFile();
		return String.format("%s.%s",
				desc2 == null ? null : desc2.getName(),
				desc1 == null ? null : desc1.getName());
	}

	@Override
	public long getCommandsLastUpdatedAt() {
		return commandsLastUpdatedAt;
	}

	void markCommandsRead() {
		this.commandsLastUpdatedAt = System.currentTimeMillis();
	}

	@Override
	public void flushSlidingWindow() {
	}

	@Override
	public void flushPendingData() {
	}

	@Override
	public void attachRateLimiter(SyncRateLimiter rateLimiter) {
	}

	@Override
	public void addCommandsListener(ReplicationProgress<?> replicationProgress, CommandsListener commandsListener) {
		throw unsupported("addCommandsListener");
	}

	@Override
	public int appendCommands(ByteBuf byteBuf) {
		throw unsupported("appendCommands");
	}

	@Override
	public int onlyAppendCommand(ByteBuf byteBuf) {
		throw unsupported("onlyAppendCommand");
	}

	@Override
	public boolean awaitCommandsOffset(long offset, int timeMilli) {
		throw unsupported("awaitCommandsOffset");
	}

	@Override
	public void gc() {
		throw unsupported("gc");
	}

	@Override
	public void rotateFileIfNecessary() {
		throw unsupported("rotateFileIfNecessary");
	}

	@Override
	public void destroy() {
		throw unsupported("destroy");
	}

	@Override
	public void switchToXSync(GtidSet gtidSet) {
		throw unsupported("switchToXSync");
	}

	@Override
	public void switchToPsync(String replId, long offset) {
		throw unsupported("switchToPsync");
	}

	@Override
	public void restoreXsyncIndex() {
		throw unsupported("restoreXsyncIndex");
	}

	@Override
	public void rebindIndexWritersIfUnbound() {
		throw unsupported("rebindIndexWritersIfUnbound");
	}

	@Override
	public Pair<Long, GtidSet> locateContinueGtidSet(GtidSet gtidSet) {
		throw unsupported("locateContinueGtidSet");
	}

	@Override
	public Pair<Long, GtidSet> locateContinueGtidSetWithFallbackToEnd(GtidSet gtidSet) {
		throw unsupported("locateContinueGtidSetWithFallbackToEnd");
	}

	@Override
	public Pair<Long, GtidSet> locateTailOfCmd() {
		throw unsupported("locateTailOfCmd");
	}

	@Override
	public GtidSet getIndexGtidSet() {
		throw unsupported("getIndexGtidSet");
	}

	@Override
	public List<BacklogOffsetReplicationProgress> locateCmdSegment(String uuid, long begGno, long endGno) {
		throw unsupported("locateCmdSegment");
	}

	@Override
	public boolean retainCommands(CommandsGuarantee commandsGuarantee) {
		throw unsupported("retainCommands");
	}

	@Override
	public boolean increaseLostNotInCmdStore(GtidSet lost, IOSupplier<Boolean> supplier) {
		throw unsupported("increaseLostNotInCmdStore");
	}

	@Override
	public void resetStateForContinue() {
		throw unsupported("resetStateForContinue");
	}

	AsyncFileSystem getAsyncFileSystem() {
		return asyncFileSystem;
	}

	AsyncSegmentFile getAsyncSegmentFile() {
		return asyncSegmentFile;
	}

	File getCommandBaseDir() {
		return baseDir;
	}

	String getCommandFileNamePrefix() {
		return fileNamePrefix;
	}

	ReplId getFileSystemReplId() {
		return fileSystemReplId;
	}

	@Override
	public String toString() {
		return String.format("ReadOnlyCommandStore:%s", baseDir);
	}

	private UnsupportedOperationException unsupported(String method) {
		return new UnsupportedOperationException(getClass().getSimpleName() + "." + method);
	}
}

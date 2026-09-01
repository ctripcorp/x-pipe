package com.ctrip.xpipe.redis.keeper.store.readonly;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.utils.IOSupplier;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.store.BacklogOffsetReplicationProgress;
import com.ctrip.xpipe.redis.core.store.CommandReader;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.CommandsGuarantee;
import com.ctrip.xpipe.redis.core.store.CommandsListener;
import com.ctrip.xpipe.redis.core.store.OffsetReplicationProgress;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.core.store.ReplicationProgress;
import com.ctrip.xpipe.redis.core.store.ratelimit.SyncRateLimiter;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;
import com.ctrip.xpipe.redis.keeper.store.AbstractCommandStore;
import com.ctrip.xpipe.redis.keeper.store.AbstractStore;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.CloseState;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.ReferenceCountUtil;
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

	public static final String MONITOR_TYPE = "ReadOnlyCommandStore";

	public static final String PREPARE_WATCH_MISS = "prepareWatchMiss";

	/**
	 * {@link #addCommandsListener} 读到 null 后的防空转间隔。reopen 也不一定有新数据。
	 */
	public static final int MISS_BACKOFF_MILLI = 10;

	/**
	 * {@link #reopenAndObserve()} 距上次 close+open 不足此时长则跳过。同句柄 size 看不到新尾。
	 */
	public static final int REOPEN_DEBOUNCE_MILLI = 100;

	private static final List<String> NO_INDEX_PREFIXES = Collections.emptyList();

	@FunctionalInterface
	interface ReopenSleeper {
		void sleep(long millis) throws InterruptedException;
	}

	@FunctionalInterface
	interface MilliClock {
		long now();
	}

	private final File baseDir;

	private final String fileNamePrefix;

	private final AsyncFileSystem asyncFileSystem;

	private final ReplId fileSystemReplId;

	private final AtomicBoolean initialized = new AtomicBoolean(false);

	private final ConcurrentMap<CommandReader<?>, Boolean> readers = new ConcurrentHashMap<>();

	private final AtomicLong totalLengthSnapshot = new AtomicLong(0L);

	private volatile AsyncSegmentFile asyncSegmentFile;

	/**
	 * 共享只读句柄的互斥：close/open/list/size/pread 必须串行（FS-M5.4，禁止 close 与 pread 并发）。
	 */
	private final Object handleLock = new Object();

	private ReopenSleeper reopenSleeper = Thread::sleep;

	private MilliClock milliClock = System::currentTimeMillis;

	/**
	 * 上次实际 close+open 的时刻；负数表示还没 reopen 过。
	 */
	private long lastReopenAtMillis = -1L;

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
		synchronized (handleLock) {
			this.asyncSegmentFile = AsyncFileSystemHelper.awaitOpen(asyncFileSystem,
					() -> asyncFileSystem.open(baseDir.getAbsolutePath(), fileNamePrefix, NO_INDEX_PREFIXES, false,
							fileSystemReplId.toString()),
					"open read-only command segment " + fileNamePrefix);
			try {
				observeSizeOnCurrentHandle();
			} catch (Throwable th) {
				logger.warn("[initialize][seed fail]{}", this, th);
			}
		}
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
		synchronized (handleLock) {
			AsyncSegmentFile handle = asyncSegmentFile;
			asyncSegmentFile = null;
			AsyncFileSystemHelper.closeHandle(asyncFileSystem, handle, "close read-only command segment " + fileNamePrefix);
		}
	}

	@Override
	public long totalLength() {
		return totalLengthSnapshot.get();
	}

	/**
	 * 观察快照只前进（D30）。{@code observedEnd} 为当前段 {@code startOffset + size}。
	 * <p>
	 * 包内调用方：{@code initialize()} 种子；{@code reopenAndObserve()} 在 close+open 共享句柄之后。
	 */
	void refreshTotalLength(long observedEnd) {
		if (observedEnd < 0) {
			return;
		}
		totalLengthSnapshot.accumulateAndGet(observedEnd, Math::max);
	}

	/**
	 * 对外更新接口（D30）：reopen 共享只读句柄并刷新 {@code totalLength} 快照。
	 * 句柄只由本类访问；Watcher / Reader 调本方法或 {@link #reopenAndObserve()}，禁止直接操作句柄。
	 * 调用线程：Watcher / Reader 所在线程；禁止 Redis 命令线程（D11）。
	 * Store 不自建定时器。失败只 WARN，快照保持原值。
	 */
	public void observeCurrentEnd() {
		try {
			reopenAndObserve();
		} catch (Throwable th) {
			logger.warn("[observeCurrentEnd][fail]{}", this, th);
		}
	}

	/**
	 * close 共享句柄再 open，然后观察段 size。失败抛出，由 Reader 走 D9 分支②。
	 * 须先关尽该文件全部句柄再 open（FS-M5.4）。
	 * 距上次实际 reopen 不足 {@link #REOPEN_DEBOUNCE_MILLI} 则跳过 close+open（同句柄 size 看不到新尾）。
	 */
	void reopenAndObserve() throws IOException {
		synchronized (handleLock) {
			makeSureOpen();
			long now = milliClock.now();
			if (lastReopenAtMillis >= 0 && now - lastReopenAtMillis < REOPEN_DEBOUNCE_MILLI) {
				return;
			}
			reopenSharedHandle();
			lastReopenAtMillis = now;
			observeSizeOnCurrentHandle();
		}
	}

	ByteBuf readAt(long offset, long length) throws IOException {
		synchronized (handleLock) {
			makeSureOpen();
			if (asyncSegmentFile == null) {
				throw new IOException("no read-only command handle, offset=" + offset);
			}
			return AsyncFileSystemHelper.await(
					() -> asyncFileSystem.read(asyncSegmentFile, length, offset),
					"read read-only command " + fileNamePrefix);
		}
	}

	long startOffsetOf(long readOffset) {
		synchronized (handleLock) {
			if (asyncSegmentFile == null) {
				return -1L;
			}
			return asyncFileSystem.getStartOffsetByReadOffset(asyncSegmentFile, readOffset);
		}
	}

	private void reopenSharedHandle() throws IOException {
		AsyncSegmentFile previous = asyncSegmentFile;
		this.asyncSegmentFile = null;
		AsyncFileSystemHelper.closeHandle(asyncFileSystem, previous,
				"reopen read-only command shared handle " + fileNamePrefix);
		this.asyncSegmentFile = AsyncFileSystemHelper.awaitOpen(asyncFileSystem,
				() -> asyncFileSystem.open(baseDir.getAbsolutePath(), fileNamePrefix, NO_INDEX_PREFIXES, false,
						fileSystemReplId.toString()),
				"reopen read-only command segment " + fileNamePrefix);
	}

	private void observeSizeOnCurrentHandle() throws IOException {
		if (asyncSegmentFile == null) {
			throw new IOException("no read-only command handle to observe");
		}
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
	}

	@Override
	public long lowestAvailableOffset() {
		synchronized (handleLock) {
			makeSureOpen();
			List<Long> segmentOffsets = asyncFileSystem.list(asyncSegmentFile);
			if (segmentOffsets == null || segmentOffsets.isEmpty()) {
				logger.info("[lowestAvailableOffset][no cmd segments][start offset 0]");
				return 0L;
			}
			return segmentOffsets.get(0);
		}
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
		// 生产唯一调用方是 doInitialize 的 resetReplAfterLongTimeDown，只读 Store 不会走到
		return 0L;
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
	public void addCommandsListener(ReplicationProgress<?> progress, CommandsListener listener) throws IOException {
		if (!(progress instanceof OffsetReplicationProgress) && !(progress instanceof BacklogOffsetReplicationProgress)) {
			throw new UnsupportedOperationException(getClass().getSimpleName() + ".addCommandsListener unsupported progress "
					+ progress);
		}

		makeSureOpen();
		logger.info("[addCommandsListener][begin] from offset {}, {}", progress, listener);

		CommandReader<ByteBuf> cmdReader;
		try {
			cmdReader = beginRead((ReplicationProgress<Long>) progress);
		} finally {
			listener.beforeCommand();
		}

		logger.info("[addCommandsListener] from {}, {}", progress, cmdReader);

		try {
			while (listener.isOpen() && !Thread.currentThread().isInterrupted()) {
				final ByteBuf buf = cmdReader.read(1000);
				if (buf == null) {
					missAndBackoff();
					continue;
				}

				ChannelFuture future = null;
				try {
					future = listener.onCommand(buf);
				} catch (CloseState.CloseStateException e) {
					logger.info("[addCommandsListener][listener closed] release buf");
					ReferenceCountUtil.release(buf);
					cmdReader.flushed(buf);
					throw e;
				} catch (Throwable th) {
					ReferenceCountUtil.release(buf);
					cmdReader.flushed(buf);
					throw th;
				}

				if (future != null) {
					CommandReader<ByteBuf> finalCmdReader = cmdReader;
					future.addListener(new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture completeFuture) {
							if (!completeFuture.isSuccess()) {
								logger.error("[onCommand][err] {}", buf, completeFuture.cause());
							}
							finalCmdReader.flushed(buf);
						}
					});
				} else {
					ReferenceCountUtil.release(buf);
					cmdReader.flushed(buf);
				}
			}
		} catch (Throwable th) {
			logger.error("[addCommandsListener][exit]{}", listener, th);
			if (th instanceof IOException) {
				throw (IOException) th;
			}
			if (th instanceof RuntimeException) {
				throw (RuntimeException) th;
			}
			if (th instanceof Error) {
				throw (Error) th;
			}
			throw new IOException("read-only command listener exit", th);
		} finally {
			cmdReader.close();
		}
		logger.info("[addCommandsListener][end] from {}, {}", progress, listener);
	}

	private CommandReader<ByteBuf> beginRead(ReplicationProgress<Long> progress) {
		makeSureOpen();
		CommandReader<ByteBuf> reader = new ReopenOffsetCommandReader(
				progress.getProgress(), this,
				AbstractCommandStore.DEFAULT_COMMAND_READER_FLYING_THRESHOLD);
		addReader(reader);
		return reader;
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

	void missAndBackoff() {
		if (isForbiddenBackoffThread(Thread.currentThread())) {
			logger.error("[backoff][forbidden thread]{}", Thread.currentThread().getName());
			return;
		}
		try {
			reopenSleeper.sleep(MISS_BACKOFF_MILLI);
		} catch (InterruptedException e) {
			logger.info("[backoff][interrupted]{}", this, e);
			Thread.currentThread().interrupt();
		}
	}

	static boolean isForbiddenBackoffThread(Thread thread) {
		if (thread == null) {
			return false;
		}
		String name = thread.getName();
		if (name == null) {
			return false;
		}
		String lower = name.toLowerCase();
		return lower.contains("nioeventloop")
				|| lower.contains("nioeventloopgroup")
				|| lower.contains("commandhandler");
	}

	void setReopenSleeper(ReopenSleeper reopenSleeper) {
		this.reopenSleeper = reopenSleeper == null ? Thread::sleep : reopenSleeper;
	}

	void setMilliClock(MilliClock milliClock) {
		this.milliClock = milliClock == null ? System::currentTimeMillis : milliClock;
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

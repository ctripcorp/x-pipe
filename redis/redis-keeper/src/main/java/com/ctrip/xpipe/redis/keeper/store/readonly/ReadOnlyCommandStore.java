package com.ctrip.xpipe.redis.keeper.store.readonly;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * PREPARE 只读 CommandStore（m5 D5b / D30 / D30b）。
 * {@code implements CommandStore}，不继承生产写路径实现。
 */
public class ReadOnlyCommandStore extends AbstractStore implements CommandStore {

	private static final Logger logger = LoggerFactory.getLogger(ReadOnlyCommandStore.class);

	/**
	 * {@link #addCommandsListener} 读不到数据时的防空转间隔（D9 ①）：开相追上可见尾与关相句柄不在
	 * 是同一条路 —— 只读侧不需要「句柄一开就被唤醒」的及时性，下一轮重试晚 10ms 无影响。
	 */
	public static final int MISS_BACKOFF_MILLI = 10;

	private static final List<String> NO_INDEX_PREFIXES = Collections.emptyList();

	/**
	 * {@link #openAndObserve()} 的结果（D48 / D49）。
	 */
	public enum ObserveResult {
		/**
		 * 句柄已打开且快照已整份替换。
		 */
		OPENED,
		/**
		 * open / list / size 抛 IO 异常：内部已关句柄，保留旧快照，由 Watcher 下周期重试。
		 */
		FAILED
	}

	/**
	 * {@link #missAndBackoff()} 的可注入 sleep，单测用（句柄开关由 Watcher 驱动，Store 自己不 reopen）。
	 */
	@FunctionalInterface
	interface BackoffSleeper {
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

	/**
	 * cmd 的 offset 内存快照（D48 ④）。不可变对象整份替换，写者只有 Watcher 一条线程。
	 */
	private volatile ReadOnlyCmdOffsetSnapshot offsetSnapshot = ReadOnlyCmdOffsetSnapshot.EMPTY;

	private volatile AsyncSegmentFile asyncSegmentFile;

	/**
	 * 共享只读句柄的互斥：close/open/list/size/pread 必须串行（FS-M5.4，禁止 close 与 pread 并发）。
	 */
	private final Object handleLock = new Object();

	private BackoffSleeper backoffSleeper = Thread::sleep;

	private MilliClock milliClock = System::currentTimeMillis;

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
				// 首开是 refCount 0→1 的全新目录扫描，必然新鲜，用它种子快照（D30）；失败只 WARN 不阻断
				applyObservedSnapshot(observeSnapshotOnCurrentHandle());
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
			closeHandleLocked("close read-only command segment " + fileNamePrefix);
		}
	}

	/**
	 * 当前 offset 快照。Reader 每次 {@code doRead} 只读一次引用，区间校验与 {@code visible} 计算
	 * 都基于同一个本地引用（D49）。
	 */
	ReadOnlyCmdOffsetSnapshot offsetSnapshot() {
		return offsetSnapshot;
	}

	@Override
	public long totalLength() {
		return offsetSnapshot().getObservedEnd();
	}

	@Override
	public long lowestAvailableOffset() {
		return offsetSnapshot().getFirstOffset();
	}

	long startOffsetOf(long readOffset) {
		return offsetSnapshot().startOffsetOf(readOffset);
	}

	/**
	 * 观察快照只前进（D30）。{@code observedEnd} 为当前段 {@code startOffset + size}。
	 * <p>
	 * 段链条不变，只抬观察末尾；防御性保留，正常路径由 {@link #openAndObserve()} 整份换快照。
	 */
	void refreshTotalLength(long observedEnd) {
		if (observedEnd < 0) {
			return;
		}
		ReadOnlyCmdOffsetSnapshot current = offsetSnapshot;
		if (observedEnd <= current.getObservedEnd()) {
			return;
		}
		this.offsetSnapshot = current.withObservedEnd(observedEnd);
	}

	/**
	 * 仅诊断：是否还有 Reader。**不是** Watcher 的 reopen gate（D48，撤销 v1.16 的 {@code hasReaders()} gate）。
	 */
	public boolean hasReaders() {
		return !readers.isEmpty();
	}

	/**
	 * 关相入口（D48），只允许 {@code PrepareStoreWatcher} 调用。幂等：{@code handleLock} 内置空句柄后
	 * close；**快照保持不动**，关相期间三个 offset getter 仍返回上次观察值。
	 */
	public void closeHandleForCycle() {
		synchronized (handleLock) {
			closeHandleLocked("close read-only command segment for cycle " + fileNamePrefix);
		}
	}

	/**
	 * 开相入口（D48），只允许 {@code PrepareStoreWatcher} 调用：open → {@code list} + 末段
	 * {@code sizeOfSegment} → 整份替换快照。
	 * <p>
	 * 返回 {@link ObserveResult#FAILED} 时内部已关句柄并保留旧快照，由 Watcher 下周期重试。
	 * 本方法不抛异常，不得在 Redis 命令线程上调用（会做 FS 调用，D11）。
	 */
	public ObserveResult openAndObserve() {
		synchronized (handleLock) {
			try {
				makeSureOpen();
				if (asyncSegmentFile == null) {
					this.asyncSegmentFile = openSharedHandle();
				}
				applyObservedSnapshot(observeSnapshotOnCurrentHandle());
				return ObserveResult.OPENED;
			} catch (Throwable th) {
				logger.warn("[openAndObserve][fail][keep last snapshot]{} {}", offsetSnapshot, this, th);
				closeHandleLocked("close read-only command segment after observe fail " + fileNamePrefix);
				return ObserveResult.FAILED;
			}
		}
	}

	/**
	 * 句柄开闭的**事实**以共享句柄是否为 null 为准（D48 状态归属）。纯内存读，仅诊断与验收用；
	 * Reader **不**据此分支 —— 关相与「追上可见尾」对它是同一件事：没数据可读（D9 ①）。
	 */
	public boolean isHandleOpen() {
		return asyncSegmentFile != null;
	}

	/**
	 * 关相时句柄不在，返回 {@code null} 而**不抛**（D48 ③）；Reader 按 D9 分支 ① 处理。
	 */
	ByteBuf readAt(long offset, long length) throws IOException {
		synchronized (handleLock) {
			makeSureOpen();
			if (asyncSegmentFile == null) {
				return null;
			}
			return AsyncFileSystemHelper.await(
					() -> asyncFileSystem.read(asyncSegmentFile, length, offset),
					"read read-only command " + fileNamePrefix);
		}
	}

	/**
	 * 整份替换快照。正常路径 {@code observedEnd} 只前进（D49 的不变式已保证），这里的取 max 是防御。
	 */
	private void applyObservedSnapshot(ReadOnlyCmdOffsetSnapshot fresh) {
		if (fresh == null) {
			return;
		}
		long lastEnd = offsetSnapshot.getObservedEnd();
		this.offsetSnapshot = fresh.getObservedEnd() >= lastEnd ? fresh : fresh.withObservedEnd(lastEnd);
	}

	private void closeHandleLocked(String operation) {
		AsyncSegmentFile handle = asyncSegmentFile;
		if (handle == null) {
			return;
		}
		this.asyncSegmentFile = null;
		AsyncFileSystemHelper.closeHandle(asyncFileSystem, handle, operation);
	}

	private AsyncSegmentFile openSharedHandle() throws IOException {
		return AsyncFileSystemHelper.awaitOpen(asyncFileSystem,
				() -> asyncFileSystem.open(baseDir.getAbsolutePath(), fileNamePrefix, NO_INDEX_PREFIXES, false,
						fileSystemReplId.toString()),
				"open read-only command segment " + fileNamePrefix);
	}

	private ReadOnlyCmdOffsetSnapshot observeSnapshotOnCurrentHandle() throws IOException {
		if (asyncSegmentFile == null) {
			throw new IOException("no read-only command handle to observe");
		}
		List<Long> segmentOffsets = asyncFileSystem.list(asyncSegmentFile);
		if (segmentOffsets == null || segmentOffsets.isEmpty()) {
			return ReadOnlyCmdOffsetSnapshot.EMPTY;
		}
		long[] starts = new long[segmentOffsets.size()];
		for (int i = 0; i < starts.length; i++) {
			starts[i] = segmentOffsets.get(i);
		}
		Arrays.sort(starts);
		long lastStart = starts[starts.length - 1];
		Long segmentSize = AsyncFileSystemHelper.await(
				() -> asyncFileSystem.sizeOfSegment(asyncSegmentFile, lastStart),
				"size read-only command segment " + fileNamePrefix);
		long observedEnd = segmentSize == null || segmentSize < 0 ? lastStart : lastStart + segmentSize;
		return new ReadOnlyCmdOffsetSnapshot(observedEnd, starts, milliClock.now());
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
		// 生产唯一调用方是切 Backup 的 resetReplAfterLongTimeDown；只读 Store 不会走到
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

	/**
	 * 读不到数据时的唯一处置（D9 ①）：{@link #MISS_BACKOFF_MILLI} 退避。
	 * <p>
	 * 开相追上可见尾与关相句柄不在**不分路**：只读侧不需要「句柄一开就被唤醒」的及时性，退避本身
	 * 就是频率上界；因此不设 monitor / notify，也不持 {@link #handleLock}（Watcher 随时能关句柄）。
	 * <p>
	 * 调用方必须是每 slave 的 psync executor 或 {@code PrepareCmdParser} 线程（D18c）—— 该循环本身
	 * 就是阻塞的，不能跑在 Netty IO / 命令 handler 线程上。这条由 AC-7b 的依赖断言把关，运行时不再检查。
	 */
	void missAndBackoff() {
		try {
			backoffSleeper.sleep(MISS_BACKOFF_MILLI);
		} catch (InterruptedException e) {
			logger.info("[backoff][interrupted]{}", this, e);
			Thread.currentThread().interrupt();
		}
	}

	void setBackoffSleeper(BackoffSleeper backoffSleeper) {
		this.backoffSleeper = backoffSleeper == null ? Thread::sleep : backoffSleeper;
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

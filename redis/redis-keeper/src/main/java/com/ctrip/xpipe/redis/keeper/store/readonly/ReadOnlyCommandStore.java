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
import com.ctrip.xpipe.redis.keeper.storage.SegmentOffsetBeforeFirstException;
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
import java.util.concurrent.atomic.AtomicLong;

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
		 * 句柄已打开、不变式成立且快照已整份替换。
		 */
		OPENED,
		/**
		 * 段不连续：链条覆盖不住上次观察末尾（D49）。句柄**仍打开**、快照**整份保留**，
		 * 由 Watcher 用 30s 做「信息没更新 / cmd 非法」的二分。
		 */
		LOCATE_MISS,
		/**
		 * open / list / size 抛 IO 异常：内部已关句柄，保留旧快照，由 Watcher 下周期重试。
		 * **不计入** 30s 宽限（沿用 m3「FS 故障无限重试、不拆生产链路」的语义）。
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
	 * 连续 {@code LOCATE_MISS} 的累计次数，仅诊断与单测断言用（不打点，观测靠 WARN 日志，D49）。
	 */
	private final AtomicLong locateMissCount = new AtomicLong();

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

	public long startOffsetOf(long readOffset) {
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
	 * 仅诊断 / 单测：{@code LOCATE_MISS} 的累计次数。
	 */
	long getLocateMissCount() {
		return locateMissCount.get();
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
	 * {@code sizeOfSegment} → 校验不变式 → 整份替换快照。
	 * <p>
	 * 三态（D49）：
	 * <ul>
	 * <li>{@link ObserveResult#OPENED} —— 不变式成立，快照整份替换；</li>
	 * <li>{@link ObserveResult#LOCATE_MISS} —— 段不连续，句柄仍开、快照整份保留，打点 + WARN；</li>
	 * <li>{@link ObserveResult#FAILED} —— FS 抛异常，内部已关句柄并保留旧快照。</li>
	 * </ul>
	 * 本方法不抛异常，不得在 Redis 命令线程上调用（会做 FS 调用，D11）。
	 */
	public ObserveResult openAndObserve() {
		synchronized (handleLock) {
			ReadOnlyCmdOffsetSnapshot fresh;
			try {
				makeSureOpen();
				if (asyncSegmentFile == null) {
					this.asyncSegmentFile = openSharedHandle();
				}
				fresh = observeSnapshotOnCurrentHandle();
			} catch (Throwable th) {
				logger.warn("[openAndObserve][fail][keep last snapshot]{} {}", offsetSnapshot, this, th);
				closeHandleLocked("close read-only command segment after observe fail " + fileNamePrefix);
				return ObserveResult.FAILED;
			}
			ReadOnlyCmdOffsetSnapshot last = offsetSnapshot;
			if (hasProbe(last) && !chainCoversProbe(fresh, last.getObservedEnd())) {
				// 段不连续：现象上分不清「TFS 信息没更新」与「cmd 非法」，本轮整份沿用旧快照（含 firstOffset，
				// 跳高此时是假象），句柄留给 Watcher 一起关，由它按 30s 做二分（D49）
				locateMissCount.incrementAndGet();
				logger.warn("[openAndObserve][locate miss][keep last snapshot] last={} fresh={} {}", last, fresh, this);
				return ObserveResult.LOCATE_MISS;
			}
			applyObservedSnapshot(fresh);
			return ObserveResult.OPENED;
		}
	}

	/**
	 * 二分的阶段二（D49）：按当前段链条**重建**快照，接受它为事实。只允许 {@code PrepareStoreWatcher}
	 * 在 {@code REOPEN_LOCATE_GRACE_MILLI} 宽限用尽后调用。
	 * <p>
	 * 与 {@link #openAndObserve()} 的区别只在写快照的方式：这里是独立写入路径，**绕过**
	 * {@link #applyObservedSnapshot} 的单调 max guard —— 占槽方原地重置后 {@code observedEnd} 必须
	 * 允许回退，否则 {@code totalLength()} 会停在一个不存在的位置，Reader 会读到垃圾；{@code firstOffset}
	 * 同样允许跳高。
	 * <p>
	 * **不关句柄、不 release Store、不产生任何中断信号**：落在空洞里（{@code < firstOffset}）或新尾右边
	 * （{@code > observedEnd}）的 Reader 由 {@code ReopenOffsetCommandReader} 的快照区间校验自行失效，
	 * 仍落在区间内的 Reader 继续读是安全的（链条恒连续）。
	 *
	 * @return 是否重建成功；失败（句柄不在 / FS 抛错）只 WARN 并保留旧快照，由 Watcher 下周期重试
	 */
	public boolean rebuildSnapshotFromCurrentChain() {
		synchronized (handleLock) {
			try {
				makeSureOpen();
				if (asyncSegmentFile == null) {
					logger.warn("[rebuildSnapshotFromCurrentChain][no handle][keep last snapshot]{} {}",
							offsetSnapshot, this);
					return false;
				}
				ReadOnlyCmdOffsetSnapshot rebuilt = observeSnapshotOnCurrentHandle();
				logger.info("[rebuildSnapshotFromCurrentChain] last={} rebuilt={} {}", offsetSnapshot, rebuilt, this);
				this.offsetSnapshot = rebuilt;
				return true;
			} catch (Throwable th) {
				logger.warn("[rebuildSnapshotFromCurrentChain][fail][keep last snapshot]{} {}",
						offsetSnapshot, this, th);
				return false;
			}
		}
	}

	/**
	 * 是否有 probe 可校验（D49）。{@link ReadOnlyCmdOffsetSnapshot#EMPTY} 是**「从未成功观察过」的哨兵**
	 * —— {@code initialize()} 的种子观察允许失败，此时 {@code observedEnd == 0} 并不表示「观察到末尾是 0」，
	 * 拿它当 probe 会在占槽方已 GC 过前缀（{@code segs[0] > 0}）时把首次观察误判成 {@code LOCATE_MISS}，
	 * 白等一个 30s 宽限。
	 * <p>
	 * 用**引用相等**区分：{@link #observeSnapshotOnCurrentHandle()} 观察到空目录时返回带
	 * {@code observedAtMillis} 的新实例，绝不返回该常量。因此「观察到过空目录、随后出现起点 > 0 的链条」
	 * 仍是真不连续，照判 {@code LOCATE_MISS}。
	 */
	private boolean hasProbe(ReadOnlyCmdOffsetSnapshot last) {
		return last != ReadOnlyCmdOffsetSnapshot.EMPTY;
	}

	/**
	 * D49 的一条不变式：{@code segs[0] <= probe <= segs[last] + sizeOfSegment(segs[last])}，
	 * {@code probe} 取上次快照的 {@code observedEnd}。
	 * <p>
	 * 一条式子同时覆盖「老段掉链」「前缀被删」「末段 size 回退」，并顺带保证 {@code observedEnd} 单调不减。
	 * {@code segs} 为空时：{@code probe == 0} 是合法空目录（算 {@code OPENED}，不推进），
	 * {@code probe > 0} 说明观察到的目录比上次还空，走二分。
	 */
	private boolean chainCoversProbe(ReadOnlyCmdOffsetSnapshot fresh, long probe) {
		if (fresh.getSegmentCount() == 0) {
			return probe == 0;
		}
		return fresh.getFirstOffset() <= probe && probe <= fresh.getObservedEnd();
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
	 * <p>
	 * {@link SegmentOffsetBeforeFirstException} 是 Reader 侧的**单点**残留（D49）：快照可能比 FS 实际
	 * 状态旧，区间校验通过后占槽方的 GC 仍可能已把这个 offset 所在前缀删掉。它由
	 * {@code TailCacheFileSystem.readInternal} 的 {@code fsPrepare} 内联同步抛出，不经
	 * {@code AsyncFileSystemHelper.await} 的 future 包装，这里明确转 {@link IOException}：
	 * 语义是**只断本 Reader**（D9 分支 ②），**不做宽限** —— 全局的段不连续已由 {@link #openAndObserve()}
	 * 的不变式与 Watcher 的 30s 二分拦住，与本条无关。
	 */
	ByteBuf readAt(long offset, long length) throws IOException {
		synchronized (handleLock) {
			makeSureOpen();
			if (asyncSegmentFile == null) {
				return null;
			}
			try {
				return AsyncFileSystemHelper.await(
						() -> asyncFileSystem.read(asyncSegmentFile, length, offset),
						"read read-only command " + fileNamePrefix);
			} catch (SegmentOffsetBeforeFirstException e) {
				throw new IOException("read-only command offset " + offset + " before first segment, snapshot="
						+ offsetSnapshot + ", " + this, e);
			}
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
			// 观察到的空目录是**事实**，不能返回 EMPTY 常量 —— 那个身份留给「从未观察过」（见 hasProbe）
			return new ReadOnlyCmdOffsetSnapshot(0L, new long[0], milliClock.now());
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

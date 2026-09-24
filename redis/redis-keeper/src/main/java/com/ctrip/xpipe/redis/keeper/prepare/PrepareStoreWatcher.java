package com.ctrip.xpipe.redis.keeper.prepare;

import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.MetaStore;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.core.store.ReplStage;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import com.ctrip.xpipe.redis.keeper.store.meta.AbstractMetaStore;
import com.ctrip.xpipe.redis.keeper.store.readonly.ReadOnlyCommandStore;
import com.ctrip.xpipe.redis.keeper.util.KeeperReplIdAwareThreadFactory;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * PREPARE 只读 Store 的变更感知者，同时是**只读句柄的唯一所有者**（D8 / D48 / §4.2.3）。
 * <p>
 * 单 scheduled 线程，固定 {@link #WATCH_TICK_MILLI} 的 {@code scheduleWithFixedDelay}，两相状态机：
 * <ul>
 * <li><b>开相</b>满 {@code keeper.prepare.watch.reopen.interval.milli} → 关三个只读句柄
 * （cmd → {@code meta.v2.json} → {@code store_manager_meta.properties}）进关相；</li>
 * <li><b>关相</b>满 {@code keeper.prepare.watch.close.hold.milli}（FS-M5.5 的静置窗口）→ 重读
 * {@code latest.store.dir} 做换店判定、reload meta、{@code openAndObserve()} 回开相。</li>
 * </ul>
 * 两个阈值都用**时钟**判定，不依赖调度精度：配置值再怎么变也不会把 close→open 压到静置窗口以内。
 * <p>
 * 感知语义不变：未打开不 {@code getCurrent()}；换店只 {@code releaseCurrentStore()} + 断 slave，
 * 打开由请求侧触发。reopen **不看** {@code hasReaders()}，无 Reader 也照常 cycle（D48）。
 * <p>
 * open 之后还要过一条不变式（D49）：段链条必须覆盖上次观察到的末尾。不成立时以
 * {@link #REOPEN_LOCATE_GRACE_MILLI} 做二分 —— 之内当「TFS 信息没更新」关句柄重试，之外当「cmd 非法」
 * 重建 offset 快照。
 */
public class PrepareStoreWatcher {

	private static final Logger logger = LoggerFactory.getLogger(PrepareStoreWatcher.class);

	/**
	 * Watcher 唤醒间隔，**固定不可配**（D48）：两个相位阈值用时钟判定，tick 只决定判定粒度。
	 */
	public static final int WATCH_TICK_MILLI = 1000;

	/**
	 * 段不连续的判据分界（D49）：之内按「TFS 信息没更新」重试，之外按「cmd 非法」重建快照。
	 * <p>
	 * 只读侧看到「段不连续」时无法从现象上区分这两者，而处置互斥 —— 前者必须保持旧快照等它自愈，
	 * 后者必须丢弃旧快照，否则 Reader 会跨着空洞把不连续的字节当连续流交出去。故用时间做二分。
	 */
	public static final int REOPEN_LOCATE_GRACE_MILLI = 30000;

	/**
	 * {@link #locateMissSinceMillis} 的空值。用 {@code -1} 而非 {@code 0}：注入时钟的起点可以是 0。
	 */
	private static final long NO_LOCATE_MISS = -1L;

	/**
	 * 只读句柄的相位（D48）。
	 */
	enum Phase {
		/**
		 * 句柄打开，Reader 正常消费。
		 */
		OPEN,
		/**
		 * 句柄全关且静置中，读请求一律走内存快照。
		 */
		CLOSED
	}

	@FunctionalInterface
	interface MilliClock {
		long now();
	}

	private final ReplicationStoreManager manager;

	private final KeeperConfig keeperConfig;

	private final PrepareStoreChangeListener changeListener;

	private final ReplId replId;

	private volatile PrepareWatchSnapshot snapshot;

	private ScheduledExecutorService scheduled;

	private ScheduledFuture<?> watchFuture;

	private final AtomicLong pollCount = new AtomicLong();

	private final AtomicLong storeSwitchedCount = new AtomicLong();

	private final AtomicLong chainRebuiltCount = new AtomicLong();

	private MilliClock clock = System::currentTimeMillis;

	/**
	 * 相位状态全在 Watcher（D48 状态归属）；只由 watch 线程写，命令线程不读。
	 */
	private volatile Phase phase = Phase.OPEN;

	private volatile long lastOpenAtMillis;

	private volatile long lastCloseAtMillis;

	/**
	 * 连续 {@code LOCATE_MISS} 的起点（D49 的二分计时）；{@link #NO_LOCATE_MISS} 表示当前没有在计时。
	 */
	private volatile long locateMissSinceMillis = NO_LOCATE_MISS;

	/**
	 * 当前已绑定的 Store 实例，用于识别「请求侧新开了一个店」。
	 */
	private ReplicationStore boundStore;

	/**
	 * 仅单测注入下一轮 poll 失败，验证周期任务兜住异常后下一轮仍执行。
	 */
	private volatile RuntimeException failNextPoll;

	public PrepareStoreWatcher(ReplicationStoreManager manager, KeeperConfig keeperConfig,
							   PrepareStoreChangeListener changeListener) {
		this.manager = Objects.requireNonNull(manager, "manager");
		this.keeperConfig = Objects.requireNonNull(keeperConfig, "keeperConfig");
		this.changeListener = changeListener == null ? PrepareStoreChangeListener.NOOP : changeListener;
		this.replId = manager.getReplId();
		this.lastOpenAtMillis = clock.now();
	}

	public synchronized void start() {
		if (scheduled != null) {
			return;
		}
		enterOpenPhase(clock.now());
		scheduled = Executors.newSingleThreadScheduledExecutor(
				KeeperReplIdAwareThreadFactory.create(replId, "prepare-watch"));
		watchFuture = scheduled.scheduleWithFixedDelay(this::safePoll, 0, WATCH_TICK_MILLI, TimeUnit.MILLISECONDS);
		logger.info("[start] tick={}ms reopenInterval={}ms closeHold={}ms {}",
				WATCH_TICK_MILLI, reopenIntervalMilli(), closeHoldMilli(), this);
	}

	public synchronized void stop() {
		if (watchFuture != null) {
			watchFuture.cancel(true);
			watchFuture = null;
		}
		if (scheduled != null) {
			scheduled.shutdownNow();
			scheduled = null;
		}
		logger.info("[stop]{}", this);
	}

	/**
	 * 命令线程只读。未打开或从未刷新时返回 {@code null}。
	 */
	public PrepareWatchSnapshot getSnapshot() {
		return snapshot;
	}

	/**
	 * 仅单测：换店次数。
	 */
	@VisibleForTesting
	long getStoreSwitchedCount() {
		return storeSwitchedCount.get();
	}

	@VisibleForTesting
	long getPollCount() {
		return pollCount.get();
	}

	/**
	 * 仅单测：按「cmd 非法」重建快照的次数（D49 阶段二）。
	 */
	@VisibleForTesting
	long getChainRebuiltCount() {
		return chainRebuiltCount.get();
	}

	/**
	 * 仅单测：连续 {@code LOCATE_MISS} 的起点，{@link #NO_LOCATE_MISS} 表示未在计时。
	 */
	@VisibleForTesting
	long getLocateMissSinceMillis() {
		return locateMissSinceMillis;
	}

	/**
	 * 仅单测：让下一轮 {@link #pollOnce()} 抛出指定异常。
	 */
	@VisibleForTesting
	void failNextPoll(RuntimeException fail) {
		this.failNextPoll = fail;
	}

	/**
	 * 仅单测：注入时钟，两个相位阈值由它判定。注入后从「刚 open」重新计时。
	 */
	@VisibleForTesting
	void setClock(MilliClock clock) {
		this.clock = clock == null ? System::currentTimeMillis : clock;
		enterOpenPhase(this.clock.now());
	}

	@VisibleForTesting
	Phase getPhase() {
		return phase;
	}

	@VisibleForTesting
	void pollOnce() throws IOException {
		pollCount.incrementAndGet();
		RuntimeException injected = failNextPoll;
		if (injected != null) {
			failNextPoll = null;
			throw injected;
		}

		long now = clock.now();
		rebindIfCurrentStoreChanged(now);

		if (phase == Phase.OPEN) {
			pollOpenPhase(now);
		} else {
			pollClosedPhase(now);
		}
	}

	/**
	 * 开相：到点就关掉三个句柄进关相，其余什么都不做（Reader 正常消费，快照保持）。
	 */
	private void pollOpenPhase(long now) {
		if (now - lastOpenAtMillis < reopenIntervalMilli()) {
			return;
		}
		closeReadOnlyHandles();
		enterClosedPhase(now);
	}

	/**
	 * 关相：静置满 {@code close.hold} 才允许 open（FS-M5.5）。转相只在这里发生一次，
	 * 「能不能进开相」由 {@link #reopenReadOnlyHandles()} 一个方法回答。
	 */
	private void pollClosedPhase(long now) throws IOException {
		if (now - lastCloseAtMillis < closeHoldMilli()) {
			return;
		}
		if (reopenReadOnlyHandles(now)) {
			enterOpenPhase(now);
		} else {
			// 句柄没能打开：重新计静置时长，下一轮再试
			enterClosedPhase(now);
		}
	}

	/**
	 * 静置窗口已过，做本轮该做的事：换店判定 → reload meta → open + 观察 → 段不连续的二分。
	 *
	 * @return 是否可以进开相；{@code false} 表示句柄仍未打开，需要继续静置后重试
	 */
	private boolean reopenReadOnlyHandles(long now) throws IOException {
		String latestDir = manager.reloadLatestStoreDir();
		ReplicationStore current = manager.getOpenedStore();
		if (current == null) {
			// 没打开店：只 cycle manager meta 句柄，不 getCurrent()
			return true;
		}
		if (latestDir != null && !sameStoreDir(current, latestDir)) {
			releaseSwitchedStore(current, latestDir);
			return true;
		}

		reloadStoreMeta(current);
		ReadOnlyCommandStore cmdStore = readOnlyCmdStore(current);
		if (cmdStore == null) {
			refreshSnapshot();
			return true;
		}
		ReadOnlyCommandStore.ObserveResult observed = cmdStore.openAndObserve();
		if (observed == ReadOnlyCommandStore.ObserveResult.OPENED) {
			clearLocateMiss();
			refreshSnapshot();
			return true;
		}
		if (observed == ReadOnlyCommandStore.ObserveResult.LOCATE_MISS) {
			return onLocateMiss(cmdStore, now);
		}
		// FS 故障：cmd 句柄已由 Store 关掉，meta 两个句柄一起关，保证下轮是完整 close→静置→open。
		// 不动 LOCATE_MISS 计时 —— FAILED 不计入 30s 宽限（D49）
		closeReadOnlyHandles();
		return false;
	}

	/**
	 * 段不连续的二分（D49）。
	 * <ul>
	 * <li><b>30s 内</b>按「TFS 信息没更新」：关三个句柄退回关相（保证下轮是完整 close→静置→open），
	 * 约 1s 后重试，快照整份保留，**不碰任何 Reader** —— 它们只是没有新数据可读；</li>
	 * <li><b>满 30s</b>按「cmd 非法」：接受当前链条为事实重建快照（允许 {@code observedEnd} 回退），
	 * ERROR 日志，句柄**保持打开**、**不** {@code onStoreChanged}、**不** {@code releaseCurrentStore()}
	 * —— 目录没换，没有理由拆店；落在空洞 / 新尾右边的 Reader 由快照区间校验自行失效。</li>
	 * </ul>
	 *
	 * @return 是否可以进开相
	 */
	private boolean onLocateMiss(ReadOnlyCommandStore cmdStore, long now) {
		if (locateMissSinceMillis == NO_LOCATE_MISS) {
			this.locateMissSinceMillis = now;
		}
		long elapsed = now - locateMissSinceMillis;
		if (elapsed < REOPEN_LOCATE_GRACE_MILLI) {
			logger.warn("[locateMiss][retry] elapsed={}ms grace={}ms {}", elapsed, REOPEN_LOCATE_GRACE_MILLI, this);
			closeReadOnlyHandles();
			return false;
		}
		logger.error("[locateMiss][cmd chain illegal][rebuild snapshot] elapsed={}ms grace={}ms {}",
				elapsed, REOPEN_LOCATE_GRACE_MILLI, this);
		if (!cmdStore.rebuildSnapshotFromCurrentChain()) {
			// 重建自己也失败（句柄不在 / FS 抛错）：保留计时与旧快照，按 FS 故障那条路下一轮再试
			closeReadOnlyHandles();
			return false;
		}
		chainRebuiltCount.incrementAndGet();
		clearLocateMiss();
		refreshSnapshot();
		return true;
	}

	private void clearLocateMiss() {
		this.locateMissSinceMillis = NO_LOCATE_MISS;
	}

	/**
	 * 占槽者换店：断 slave + 释放。本轮不重建，新店由请求侧 {@code getCurrent()} 打开。
	 */
	private void releaseSwitchedStore(ReplicationStore current, String latestDir) throws IOException {
		logger.info("[storeSwitched]latest.store.dir current={} latest={} {}",
				storeDirName(current), latestDir, this);
		storeSwitchedCount.incrementAndGet();
		changeListener.onStoreChanged("latest.store.dir");
		manager.releaseCurrentStore();
		this.boundStore = null;
		this.snapshot = null;
		// 店都没了，旧店上的定位计时没有意义
		clearLocateMiss();
	}

	/**
	 * 请求侧在关相里新开了一个店时重绑并视为「刚 open」（D48）：新 Store 的 {@code initialize()} 刚做过
	 * refCount 0→1 的全新扫描，本就新鲜，不需要立刻再 cycle 一次。
	 */
	private void rebindIfCurrentStoreChanged(long now) {
		ReplicationStore current = manager.getOpenedStore();
		if (current == boundStore) {
			return;
		}
		this.boundStore = current;
		clearLocateMiss();
		if (current == null) {
			return;
		}
		logger.info("[rebind]current={} {}", storeDirName(current), this);
		enterOpenPhase(now);
	}

	private void enterOpenPhase(long now) {
		this.phase = Phase.OPEN;
		this.lastOpenAtMillis = now;
	}

	private void enterClosedPhase(long now) {
		this.phase = Phase.CLOSED;
		this.lastCloseAtMillis = now;
	}

	private int reopenIntervalMilli() {
		return Math.max(0, keeperConfig.getPrepareWatchReopenIntervalMilli());
	}

	private int closeHoldMilli() {
		return Math.max(0, keeperConfig.getPrepareWatchCloseHoldMilli());
	}

	/**
	 * 关相：逐个兜异常，任何一个失败都不能挡住其它句柄与下一轮。
	 */
	private void closeReadOnlyHandles() {
		ReplicationStore current = manager.getOpenedStore();
		if (current != null) {
			ReadOnlyCommandStore cmdStore = readOnlyCmdStore(current);
			if (cmdStore != null) {
				try {
					cmdStore.closeHandleForCycle();
				} catch (Throwable th) {
					logger.warn("[closeReadOnlyHandles][cmd]{}", this, th);
				}
			}
			MetaStore metaStore = current.getMetaStore();
			if (metaStore instanceof AbstractMetaStore) {
				try {
					((AbstractMetaStore) metaStore).closeReadOnlyMetaHandle();
				} catch (Throwable th) {
					logger.warn("[closeReadOnlyHandles][meta]{}", this, th);
				}
			}
		}
		try {
			manager.closeReadOnlyMetaHandle();
		} catch (Throwable th) {
			logger.warn("[closeReadOnlyHandles][managerMeta]{}", this, th);
		}
	}

	private void reloadStoreMeta(ReplicationStore current) {
		MetaStore metaStore = current.getMetaStore();
		if (metaStore instanceof AbstractMetaStore) {
			((AbstractMetaStore) metaStore).reloadReadOnlyMeta();
		}
	}

	private static ReadOnlyCommandStore readOnlyCmdStore(ReplicationStore store) {
		if (!(store instanceof DefaultReplicationStore)) {
			return null;
		}
		CommandStore cmdStore = ((DefaultReplicationStore) store).getCommandStore();
		return cmdStore instanceof ReadOnlyCommandStore ? (ReadOnlyCommandStore) cmdStore : null;
	}

	private void safePoll() {
		try {
			pollOnce();
		} catch (Throwable th) {
			logger.error("[watch]{}", this, th);
		}
	}

	private void refreshSnapshot() {
		ReplicationStore store = manager.getOpenedStore();
		if (!(store instanceof DefaultReplicationStore)) {
			this.snapshot = null;
			return;
		}
		// 观察发生在 ReadOnlyCommandStore.openAndObserve()（与有无 Reader 无关，D48）；这里只读内存快照
		CommandStore cmdStore = ((DefaultReplicationStore) store).getCommandStore();
		long totalLength = cmdStore == null ? 0L : cmdStore.totalLength();
		long backlogEnd = store.backlogEndOffset();
		long backlogBegin = store.backlogBeginOffset();
		if (backlogBegin < 0) {
			backlogBegin = 0L;
		}
		long replOffset = 0L;
		String masterReplId = ReplicationStoreMeta.EMPTY_REPL_ID;
		String masterReplId2 = ReplicationStoreMeta.EMPTY_REPL_ID;
		long secondReplOffset = ReplicationStoreMeta.DEFAULT_SECOND_REPLID_OFFSET;
		MetaStore metaStore = store.getMetaStore();
		ReplStage curStage = metaStore == null ? null : metaStore.getCurrentReplStage();
		if (curStage != null) {
			// Same inclusive last-byte formula as DefaultReplicationStore.getCurReplStageReplOff()
			replOffset = curStage.getBegOffsetRepl() - 1 + backlogEnd - curStage.getBegOffsetBacklog();
			if (curStage.getReplId() != null) {
				masterReplId = curStage.getReplId();
			}
			if (curStage.getReplId2() != null) {
				masterReplId2 = curStage.getReplId2();
			}
			secondReplOffset = curStage.getSecondReplIdOffset();
		} else if (metaStore != null) {
			if (metaStore.getReplId() != null) {
				masterReplId = metaStore.getReplId();
			}
			if (metaStore.getReplId2() != null) {
				masterReplId2 = metaStore.getReplId2();
			}
			if (metaStore.getSecondReplIdOffset() != null) {
				secondReplOffset = metaStore.getSecondReplIdOffset();
			}
		}
		this.snapshot = new PrepareWatchSnapshot(totalLength, backlogEnd, replOffset,
				masterReplId, masterReplId2, secondReplOffset, backlogBegin);
	}

	private static boolean sameStoreDir(ReplicationStore current, String latestDir) {
		return latestDir.equals(storeDirName(current));
	}

	private static String storeDirName(ReplicationStore store) {
		if (!(store instanceof DefaultReplicationStore)) {
			return null;
		}
		File storeDir = ((DefaultReplicationStore) store).getBaseDir();
		return storeDir == null ? null : storeDir.getName();
	}

	@Override
	public String toString() {
		return String.format("PrepareStoreWatcher:%s", replId);
	}
}

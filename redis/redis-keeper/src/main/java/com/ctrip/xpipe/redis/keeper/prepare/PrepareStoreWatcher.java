package com.ctrip.xpipe.redis.keeper.prepare;

import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
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
 * PREPARE 只读 Store 的 meta 轮询（D8 / §4.2.3）。单 scheduled 线程，不持 cmd 句柄。
 * 只感知 + 释放：未打开不 {@code getCurrent()}；换店只 {@code releaseCurrentStore()}，打开由请求侧触发。
 */
public class PrepareStoreWatcher {

	private static final Logger logger = LoggerFactory.getLogger(PrepareStoreWatcher.class);

	private final ReplicationStoreManager manager;

	private final KeeperConfig keeperConfig;

	private final PrepareStoreChangeListener changeListener;

	private final ReplId replId;

	private volatile PrepareWatchSnapshot snapshot;

	private ScheduledExecutorService scheduled;

	private ScheduledFuture<?> watchFuture;

	private final AtomicLong pollCount = new AtomicLong();

	private final AtomicLong storeSwitchedCount = new AtomicLong();

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
	}

	public synchronized void start() {
		if (scheduled != null) {
			return;
		}
		int interval = Math.max(1, keeperConfig.getPrepareWatchMetaIntervalMilli());
		scheduled = Executors.newSingleThreadScheduledExecutor(
				KeeperReplIdAwareThreadFactory.create(replId, "prepare-watch"));
		watchFuture = scheduled.scheduleWithFixedDelay(this::safePoll, 0, interval, TimeUnit.MILLISECONDS);
		logger.info("[start] interval={}ms {}", interval, this);
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
	 * 仅单测：让下一轮 {@link #pollOnce()} 抛出指定异常。
	 */
	@VisibleForTesting
	void failNextPoll(RuntimeException fail) {
		this.failNextPoll = fail;
	}

	@VisibleForTesting
	void pollOnce() throws IOException {
		pollCount.incrementAndGet();
		RuntimeException injected = failNextPoll;
		if (injected != null) {
			failNextPoll = null;
			throw injected;
		}

		String latestDir = manager.reloadLatestStoreDir();
		ReplicationStore opened = manager.getOpenedStore();
		if (opened != null && latestDir != null && !sameOpenedStoreDir(opened, latestDir)) {
			logger.info("[storeSwitched]latest.store.dir opened={} latest={} {}",
					openedStoreDirName(opened), latestDir, this);
			storeSwitchedCount.incrementAndGet();
			changeListener.onStoreChanged("latest.store.dir");
			manager.releaseCurrentStore();
		}
		refreshSnapshot();
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
		CommandStore cmdStore = ((DefaultReplicationStore) store).getCommandStore();
		if (cmdStore instanceof ReadOnlyCommandStore) {
			ReadOnlyCommandStore readOnly = (ReadOnlyCommandStore) cmdStore;
			if (!readOnly.hasReaders()) {
				readOnly.observeCurrentEnd();
			}
		}
		long totalLength = cmdStore == null ? 0L : cmdStore.totalLength();
		this.snapshot = new PrepareWatchSnapshot(totalLength, store.backlogEndOffset());
	}

	private static boolean sameOpenedStoreDir(ReplicationStore opened, String latestDir) {
		String openedName = openedStoreDirName(opened);
		return latestDir.equals(openedName);
	}

	private static String openedStoreDirName(ReplicationStore opened) {
		if (!(opened instanceof DefaultReplicationStore)) {
			return null;
		}
		File storeDir = ((DefaultReplicationStore) opened).getBaseDir();
		return storeDir == null ? null : storeDir.getName();
	}

	@Override
	public String toString() {
		return String.format("PrepareStoreWatcher:%s", replId);
	}
}

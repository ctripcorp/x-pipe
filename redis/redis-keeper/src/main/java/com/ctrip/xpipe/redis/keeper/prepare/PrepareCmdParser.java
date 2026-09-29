package com.ctrip.xpipe.redis.keeper.prepare;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.store.BacklogOffsetReplicationProgress;
import com.ctrip.xpipe.redis.core.store.CommandsListener;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.config.KeeperPubSubConstants;
import com.ctrip.xpipe.redis.keeper.pubsub.KeeperPubSubListener;
import com.ctrip.xpipe.redis.keeper.pubsub.KeeperPubSubParseHook;
import com.ctrip.xpipe.redis.keeper.util.KeeperReplIdAwareThreadFactory;
import com.ctrip.xpipe.utils.CloseState;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * PREPARE 命令流 PUBLISH 解析（D17 / D18b / §4.4.2）。
 * 独占线程，自己 {@code getCurrent()} 后再 {@code addCommandsListener}；
 * 换店 release 后同样自己再打开。不由 Watcher / Store / PubSubRegistry 持有。
 * {@link #stop()} 不 join：切主路径不得卡住；listener close 后 {@code onCommand} 抛
 * {@link CloseState.CloseStateException} 结束阻塞循环。
 */
public class PrepareCmdParser {

	private static final Logger logger = LoggerFactory.getLogger(PrepareCmdParser.class);

	private final ReplicationStoreManager manager;

	private final KeeperPubSubParseHook hook;

	private final ReplId replId;

	private volatile boolean running;

	private Thread thread;

	private volatile CmdListener currentListener;

	private final AtomicLong attachCount = new AtomicLong();

	public PrepareCmdParser(ReplicationStoreManager manager, RedisOpParser redisOpParser,
							KeeperPubSubListener listener) {
		this.manager = Objects.requireNonNull(manager, "manager");
		this.replId = manager.getReplId();
		this.hook = new KeeperPubSubParseHook(redisOpParser, listener);
	}

	public synchronized void start() {
		if (running) {
			return;
		}
		running = true;
		thread = KeeperReplIdAwareThreadFactory.create(replId, "prepare-cmd-parser")
				.newThread(this::runLoop);
		thread.start();
		logger.info("[start]{}", this);
	}

	public synchronized void stop() {
		running = false;
		CmdListener listener = this.currentListener;
		if (listener != null) {
			listener.close();
		}
		if (thread != null) {
			thread.interrupt();
			thread = null;
		}
		hook.reset();
		logger.info("[stop]{}", this);
	}

	@VisibleForTesting
	long getAttachCount() {
		return attachCount.get();
	}

	@VisibleForTesting
	public boolean isRunning() {
		return running;
	}

	private void runLoop() {
		while (running) {
			try {
				if (!manager.isReadOnly()) {
					logger.info("[runLoop][not read-only] {}", this);
					break;
				}
				ReplicationStore store = manager.getCurrent();
				if (!running) {
					break;
				}
				if (store == null) {
					sleepQuietly(KeeperPubSubConstants.PREPARE_CMD_PARSER_RETRY_MILLI);
					continue;
				}
				if (!manager.isReadOnly()) {
					logger.info("[runLoop][not read-only after getCurrent] {}", this);
					break;
				}
				hook.reset();
				long start = Math.max(0, store.backlogEndOffset());
				CmdListener listener = new CmdListener();
				currentListener = listener;
				attachCount.incrementAndGet();
				logger.info("[addCommandsListener] from backlog {} {}", start, this);
				store.addCommandsListener(new BacklogOffsetReplicationProgress(start), listener);
			} catch (Throwable th) {
				if (!running) {
					break;
				}
				logger.warn("[runLoop] retry {}", this, th);
				sleepQuietly(KeeperPubSubConstants.PREPARE_CMD_PARSER_RETRY_MILLI);
			}
		}
	}

	private static void sleepQuietly(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	@Override
	public String toString() {
		return String.format("PrepareCmdParser:%s", replId);
	}

	private class CmdListener implements CommandsListener, Closeable {

		private final CloseState closeState = new CloseState();

		@Override
		public void close() {
			closeState.setClosed();
		}

		@Override
		public boolean isOpen() {
			return running && closeState.isOpen();
		}

		@Override
		public ChannelFuture onCommand(Object cmd) {
			closeState.makeSureOpen();
			if (!running) {
				throw new CloseState.CloseStateException("prepare cmd parser stopped");
			}
			if (cmd instanceof ByteBuf) {
				hook.onCommands((ByteBuf) cmd);
			}
			return null;
		}

		@Override
		public void onCommandEnd() {
		}

		@Override
		public void beforeCommand() {
		}

		@Override
		public Long processedBacklogOffset() {
			return null;
		}
	}
}

package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.cmd.ConfigGetCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.pubsub.PublishCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.pubsub.SubscribeCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.pubsub.SubscribeListener;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * TFS Active/Prepare Pub/Sub 真实复制流验收（spec D16～D20 / D28 / AC-9～AC-12）。
 */
public class TfsKeeperPubSubTest extends AbstractTfsKeeperIntegrated {

	private static final int COMMAND_TIMEOUT_MILLI = 5000;

	@Override
	protected KeeperConfig getKeeperConfig() {
		TestKeeperConfig config = (TestKeeperConfig) super.getKeeperConfig();
		config.setPubsubParseEnabled(true);
		return config;
	}

	@Test
	public void testMasterPublishReachesActiveAndPrepareExactlyOnce() throws Throwable {
		DefaultRedisKeeperServer active = tfsKeeperServer(activeKeeper);
		DefaultRedisKeeperServer prepare = tfsKeeperServer(backupKeeper);
		Assert.assertTrue(configGetPubsubParse(activeKeeper));
		Assert.assertTrue(configGetPubsubParse(backupKeeper));
		Assert.assertFalse(active.isReadOnlyStore());
		Assert.assertTrue(prepare.isReadOnlyStore());
		Assert.assertNotNull(tfsStoreManager(activeKeeper).getOpenedStore());

		String suffix = UUID.randomUUID().toString();
		String channel = "tfs-pu-" + suffix;
		String activeSentinel = "active-ready-" + suffix;
		String prepareSentinel = "prepare-ready-" + suffix;
		String payload = "master-publish-" + suffix;
		String completionPayload = "master-completion-" + suffix;
		RecordingListener activeMessages = new RecordingListener();
		RecordingListener prepareMessages = new RecordingListener();
		SubscribeCommand activeSubscribe = null;
		SubscribeCommand prepareSubscribe = null;
		CommandFuture<?> activeFuture = null;
		CommandFuture<?> prepareFuture = null;
		Throwable testFailure = null;

		try {
			activeSubscribe = subscribe(activeKeeper, channel, activeMessages);
			prepareSubscribe = subscribe(backupKeeper, channel, prepareMessages);
			activeFuture = activeSubscribe.execute();
			prepareFuture = prepareSubscribe.execute();

			waitSubscriptionReady(activeKeeper, channel, activeSentinel, activeMessages,
					activeMessages, prepareMessages);
			waitSubscriptionReady(backupKeeper, channel, prepareSentinel, prepareMessages,
					activeMessages, prepareMessages);
			activeMessages.clear();
			prepareMessages.clear();

			try (Jedis jedis = createJedis(getRedisMaster())) {
				jedis.publish(channel, payload);
				jedis.publish(channel, completionPayload);
			}
			waitForTargetAndCompletion(channel, payload, completionPayload,
					activeMessages, prepareMessages);

			List<ReceivedMessage> expectedMessages = new ArrayList<>();
			expectedMessages.add(new ReceivedMessage(channel, payload));
			expectedMessages.add(new ReceivedMessage(channel, completionPayload));
			Assert.assertEquals(expectedMessages, activeMessages.snapshot());
			Assert.assertEquals(expectedMessages, prepareMessages.snapshot());
			Assert.assertEquals(1, activeMessages.count(channel, payload));
			Assert.assertEquals(1, prepareMessages.count(channel, payload));
			Assert.assertNotNull(tfsStoreManager(backupKeeper).getOpenedStore());
		} catch (Throwable t) {
			testFailure = t;
			throw t;
		} finally {
			Throwable cleanupFailure = closeSubscription("active", activeKeeper, channel, activeSentinel,
					activeSubscribe, activeFuture, null);
			cleanupFailure = closeSubscription("prepare", backupKeeper, channel, prepareSentinel,
					prepareSubscribe, prepareFuture, cleanupFailure);
			cleanupFailure = stopPrepareAndWaitParser(prepare, cleanupFailure);
			if (cleanupFailure != null) {
				if (testFailure != null) {
					testFailure.addSuppressed(cleanupFailure);
				} else {
					throw cleanupFailure;
				}
			}
		}
	}

	private SubscribeCommand subscribe(KeeperMeta keeper, String channel, SubscribeListener listener) throws Exception {
		SubscribeCommand command = new SubscribeCommand(clientPool(keeper), scheduled,
				COMMAND_TIMEOUT_MILLI, channel);
		command.addChannelListener(listener);
		return command;
	}

	private void waitSubscriptionReady(KeeperMeta keeper, String channel, String sentinel,
			RecordingListener sideMessages, RecordingListener activeMessages,
			RecordingListener prepareMessages) throws Exception {
		try {
			waitConditionUntilTimeOut(() -> publishSentinelUntilRegistered(keeper, channel, sentinel),
					READY_WAIT_MILLI, READY_POLL_MILLI);
			waitConditionUntilTimeOut(() -> sideMessages.count(channel, sentinel) == 1,
					READY_WAIT_MILLI, READY_POLL_MILLI);
		} catch (TimeoutException e) {
			TimeoutException dump = new TimeoutException(describePubSubWait(
					"waitSubscriptionReady " + keeperEndpoint(keeper), activeMessages, prepareMessages));
			dump.initCause(e);
			throw dump;
		} catch (RuntimeException e) {
			throw new IllegalStateException(describePubSubWait(
					"waitSubscriptionReady " + keeperEndpoint(keeper), activeMessages, prepareMessages), e);
		}
	}

	private boolean publishSentinelUntilRegistered(KeeperMeta keeper, String channel, String sentinel) {
		try {
			return publishSentinel(keeper, channel, sentinel) > 0;
		} catch (Exception e) {
			throw new IllegalStateException("sentinel PUBLISH failed for " + keeperEndpoint(keeper), e);
		}
	}

	private boolean publishSentinelUntilUnregistered(KeeperMeta keeper, String channel, String sentinel) {
		try {
			return publishSentinel(keeper, channel, sentinel) == 0;
		} catch (Exception e) {
			throw new IllegalStateException("cleanup PUBLISH failed for " + keeperEndpoint(keeper), e);
		}
	}

	private int publishSentinel(KeeperMeta keeper, String channel, String sentinel) throws Exception {
		Object result = new PublishCommand(clientPool(keeper), scheduled, COMMAND_TIMEOUT_MILLI,
				channel, sentinel).execute().get(COMMAND_TIMEOUT_MILLI, TimeUnit.MILLISECONDS);
		if (!(result instanceof Number)) {
			throw new IllegalStateException("unexpected PUBLISH response: " + result);
		}
		return ((Number) result).intValue();
	}

	private void waitForTargetAndCompletion(String channel, String payload, String completionPayload,
			RecordingListener activeMessages, RecordingListener prepareMessages) throws Exception {
		try {
			waitConditionUntilTimeOut(() -> activeMessages.count(channel, payload) >= 1
						&& prepareMessages.count(channel, payload) >= 1
						&& activeMessages.count(channel, completionPayload) >= 1
						&& prepareMessages.count(channel, completionPayload) >= 1,
					READY_WAIT_MILLI, READY_POLL_MILLI);
		} catch (TimeoutException e) {
			TimeoutException dump = new TimeoutException(describePubSubWait(
					"waitForTargetAndCompletion", activeMessages, prepareMessages));
			dump.initCause(e);
			throw dump;
		}
	}

	private boolean configGetPubsubParse(KeeperMeta keeper) throws Exception {
		return new ConfigGetCommand.ConfigGetPubsubParse(clientPool(keeper), scheduled)
				.execute().get(CONFIG_GET_TIMEOUT_SECONDS, TimeUnit.SECONDS);
	}

	private SimpleObjectPool<NettyClient> clientPool(KeeperMeta keeper) throws Exception {
		return getXpipeNettyClientKeyedObjectPool().getKeyPool(keeperEndpoint(keeper));
	}

	private Throwable closeSubscription(String side, KeeperMeta keeper, String channel, String sentinel,
			SubscribeCommand command, CommandFuture<?> future, Throwable previousFailure) {
		Throwable cleanupFailure = previousFailure;
		try {
			if (command != null) {
				command.unSubscribe();
			}
		} catch (Throwable t) {
			cleanupFailure = addCleanupFailure("unsubscribe " + side, t, cleanupFailure);
		}
		try {
			if (future != null) {
				future.get(CONFIG_GET_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			}
		} catch (Throwable t) {
			cleanupFailure = addCleanupFailure("wait subscription future " + side, t, cleanupFailure);
		}
		try {
			if (command != null) {
				waitConditionUntilTimeOut(() -> publishSentinelUntilUnregistered(keeper, channel, sentinel),
						READY_WAIT_MILLI, READY_POLL_MILLI);
			}
		} catch (Throwable t) {
			cleanupFailure = addCleanupFailure("wait server unsubscribe " + side, t, cleanupFailure);
		}
		return cleanupFailure;
	}

	private Throwable stopPrepareAndWaitParser(DefaultRedisKeeperServer prepare, Throwable previousFailure) {
		try {
			com.ctrip.xpipe.lifecycle.LifecycleHelper.stopIfPossible(prepare);
			waitConditionUntilTimeOut(() -> countPrepareCmdParserThreads() == 0,
					READY_WAIT_MILLI, READY_POLL_MILLI);
			return previousFailure;
		} catch (Throwable t) {
			return addCleanupFailure("stop prepare parser", t, previousFailure);
		}
	}

	private long countPrepareCmdParserThreads() {
		return Thread.getAllStackTraces().keySet().stream()
				.filter(thread -> thread.isAlive()
						&& thread.getName() != null
						&& thread.getName().contains("prepare-cmd-parser"))
				.count();
	}

	private Throwable addCleanupFailure(String phase, Throwable failure, Throwable previousFailure) {
		logger.warn("[cleanup][{}]", phase, failure);
		if (previousFailure == null) {
			return failure;
		}
		previousFailure.addSuppressed(failure);
		return previousFailure;
	}

	private String describePubSubWait(String phase, RecordingListener activeMessages,
			RecordingListener prepareMessages) {
		return String.format("[%s] active={%s messages=%s} prepare={%s messages=%s}",
				phase, describeKeeper(activeKeeper), activeMessages.snapshot(),
				describeKeeper(backupKeeper), prepareMessages.snapshot());
	}

	private String describeKeeper(KeeperMeta keeper) {
		try {
			DefaultRedisKeeperServer server = tfsKeeperServer(keeper);
			KeeperState state = server.getRedisKeeperServerState() == null
					? null : server.getRedisKeeperServerState().keeperState();
			ReplicationStore opened = tfsStoreManager(keeper).getOpenedStore();
			return String.format("endpoint=%s state=%s readOnly=%s openedStore=%s managerBaseDir=%s",
					keeperEndpoint(keeper), state, server.isReadOnlyStore(),
					opened == null ? "null" : opened.getClass().getSimpleName(),
					tfsStoreManager(keeper).getBaseDir());
		} catch (Throwable t) {
			return "endpoint=" + keeperEndpoint(keeper) + " dumpFailed=" + t;
		}
	}

	private static final class RecordingListener implements SubscribeListener {

		private final CopyOnWriteArrayList<ReceivedMessage> messages = new CopyOnWriteArrayList<>();

		@Override
		public void message(String channel, String message) {
			messages.add(new ReceivedMessage(channel, message));
		}

		private int count(String channel, String payload) {
			int count = 0;
			for (ReceivedMessage message : messages) {
				if (Objects.equals(channel, message.channel) && Objects.equals(payload, message.payload)) {
					count++;
				}
			}
			return count;
		}

		private void clear() {
			messages.clear();
		}

		private List<ReceivedMessage> snapshot() {
			return new ArrayList<>(messages);
		}
	}

	private static final class ReceivedMessage {

		private final String channel;

		private final String payload;

		private ReceivedMessage(String channel, String payload) {
			this.channel = channel;
			this.payload = payload;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (!(o instanceof ReceivedMessage)) {
				return false;
			}
			ReceivedMessage that = (ReceivedMessage) o;
			return Objects.equals(channel, that.channel) && Objects.equals(payload, that.payload);
		}

		@Override
		public int hashCode() {
			return Objects.hash(channel, payload);
		}

		@Override
		public String toString() {
			return "(" + channel + "," + payload + ")";
		}
	}
}

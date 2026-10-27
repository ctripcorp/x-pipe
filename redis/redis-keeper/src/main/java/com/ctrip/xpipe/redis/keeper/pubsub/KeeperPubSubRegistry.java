package com.ctrip.xpipe.redis.keeper.pubsub;

import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.core.protocal.cmd.pubsub.Subscribe;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.config.KeeperPubSubConstants;
import com.ctrip.xpipe.redis.keeper.util.KeeperReplIdAwareThreadFactory;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Keeper Pub/Sub 注册表与异步投递（D16 / D18 / §4.4.1）。
 * 投递跑在独立 executor，禁止在复制 IO 线程上等待订阅者 channel 可写。
 */
public class KeeperPubSubRegistry implements AutoCloseable {

	private static final Logger logger = LoggerFactory.getLogger(KeeperPubSubRegistry.class);

	private static final String PSUBSCRIBE_PATTERN = KeeperPubSubConstants.PSUBSCRIBE_PATTERN;

	private final ReplId replId;

	private final int queueSize;

	private final ConcurrentHashMap<String, Set<RedisClient<?>>> channelSubscribers = new ConcurrentHashMap<>();

	private final Set<RedisClient<?>> patternSubscribers = ConcurrentHashMap.newKeySet();

	private final Set<RedisClient<?>> hookedClients = ConcurrentHashMap.newKeySet();

	private volatile ThreadPoolExecutor deliverExecutor;

	private volatile boolean closed;

	public KeeperPubSubRegistry(ReplId replId) {
		this(replId, KeeperPubSubConstants.PUBSUB_DELIVER_QUEUE_SIZE);
	}

	@VisibleForTesting
	public KeeperPubSubRegistry(ReplId replId, int queueSize) {
		this.replId = Objects.requireNonNull(replId, "replId");
		if (queueSize <= 0) {
			throw new IllegalArgumentException("queueSize must be positive");
		}
		this.queueSize = queueSize;
	}

	public void subscribe(RedisClient<?> client, String channel) {
		Objects.requireNonNull(client, "client");
		Objects.requireNonNull(channel, "channel");
		if (closed || !isClientActive(client)) {
			return;
		}
		channelSubscribers.computeIfAbsent(channel, key -> ConcurrentHashMap.newKeySet()).add(client);
		ensureCloseHook(client);
	}

	public void unsubscribe(RedisClient<?> client, String channel) {
		Objects.requireNonNull(client, "client");
		Objects.requireNonNull(channel, "channel");
		channelSubscribers.computeIfPresent(channel, (key, subscribers) -> {
			subscribers.remove(client);
			return subscribers.isEmpty() ? null : subscribers;
		});
	}

	public void psubscribeAll(RedisClient<?> client) {
		Objects.requireNonNull(client, "client");
		if (closed || !isClientActive(client)) {
			return;
		}
		patternSubscribers.add(client);
		ensureCloseHook(client);
	}

	public void punsubscribeAll(RedisClient<?> client) {
		Objects.requireNonNull(client, "client");
		patternSubscribers.remove(client);
	}

	public void unsubscribeAll(RedisClient<?> client) {
		Objects.requireNonNull(client, "client");
		for (String channel : channelSubscribers.keySet()) {
			unsubscribe(client, channel);
		}
		patternSubscribers.remove(client);
		hookedClients.remove(client);
	}

	public int subscribedChannelCount(RedisClient<?> client) {
		return subscribedChannels(client).size();
	}

	public boolean isSubscribed(RedisClient<?> client, String channel) {
		Objects.requireNonNull(client, "client");
		Objects.requireNonNull(channel, "channel");
		Set<RedisClient<?>> subscribers = channelSubscribers.get(channel);
		return subscribers != null && subscribers.contains(client);
	}

	public List<String> subscribedChannels(RedisClient<?> client) {
		Objects.requireNonNull(client, "client");
		List<String> channels = new ArrayList<>();
		for (Map.Entry<String, Set<RedisClient<?>>> entry : channelSubscribers.entrySet()) {
			if (entry.getValue().contains(client)) {
				channels.add(entry.getKey());
			}
		}
		return channels;
	}

	/**
	 * 查找当前订阅者并异步投递。返回成功入队的投递数（channel 订阅 + {@code PSUBSCRIBE *} 分别计数）。
	 * 队列满则丢弃并打点，返回 0，不抛不阻塞。
	 */
	public int publish(String channel, String message) {
		Objects.requireNonNull(channel, "channel");
		Objects.requireNonNull(message, "message");
		if (closed) {
			return 0;
		}
		List<RedisClient<?>> channelTargets = snapshot(channelSubscribers.get(channel));
		List<RedisClient<?>> patternTargets = snapshot(patternSubscribers);
		int receivers = channelTargets.size() + patternTargets.size();
		if (receivers == 0) {
			return 0;
		}
		ThreadPoolExecutor executor = executor();
		if (executor == null) {
			return 0;
		}
		try {
			executor.execute(() -> deliver(channel, message, channelTargets, patternTargets));
			return receivers;
		} catch (RejectedExecutionException e) {
			logger.warn("[publish][drop] channel={}, receivers={}, {}", channel, receivers, this);
			return 0;
		}
	}

	@Override
	public void close() {
		synchronized (this) {
			if (closed) {
				return;
			}
			closed = true;
			ThreadPoolExecutor executor = deliverExecutor;
			if (executor != null) {
				executor.shutdownNow();
			}
		}
	}

	private void ensureCloseHook(RedisClient<?> client) {
		if (hookedClients.add(client)) {
			client.addChannelCloseReleaseResources(() -> unsubscribeAll(client));
		}
	}

	private static boolean isClientActive(RedisClient<?> client) {
		Channel channel = client.channel();
		return channel != null && channel.isActive();
	}

	private ThreadPoolExecutor executor() {
		ThreadPoolExecutor existing = deliverExecutor;
		if (existing != null) {
			return existing;
		}
		synchronized (this) {
			if (closed) {
				return null;
			}
			if (deliverExecutor == null) {
				deliverExecutor = new ThreadPoolExecutor(
						1, 1,
						0L, TimeUnit.MILLISECONDS,
						new LinkedBlockingQueue<>(queueSize),
						KeeperReplIdAwareThreadFactory.create(replId.toString(), "pubsub-deliver", true),
						new ThreadPoolExecutor.AbortPolicy());
			}
			return deliverExecutor;
		}
	}

	private void deliver(String channel, String message, List<RedisClient<?>> channelTargets,
						 List<RedisClient<?>> patternTargets) {
		if (!channelTargets.isEmpty()) {
			fanout(channelTargets, encodeMessage(channel, message), channel);
		}
		if (!patternTargets.isEmpty()) {
			fanout(patternTargets, encodePMessage(channel, message), channel);
		}
	}

	/**
	 * 每种回包只 encode 一次。每路 {@link ByteBuf#retainedDuplicate()}，母本 finally release。
	 * 禁止对同一 ByteBuf 实例多路 writeAndFlush（readerIndex 共享且会并发改）。
	 */
	private void fanout(List<RedisClient<?>> targets, ByteBuf encoded, String channel) {
		try {
			for (RedisClient<?> client : targets) {
				sendQuietly(client, encoded.retainedDuplicate(), channel);
			}
		} finally {
			encoded.release();
		}
	}

	private void sendQuietly(RedisClient<?> client, ByteBuf payload, String channel) {
		try {
			client.sendMessage(payload);
		} catch (Throwable th) {
			logger.warn("[deliver][{}] {}", channel, client, th);
			payload.release();
		}
	}

	private static ByteBuf encodeMessage(String channel, String message) {
		return encodeArray(Subscribe.MESSAGE_TYPE.MESSAGE.desc(), channel, message);
	}

	private static ByteBuf encodePMessage(String channel, String message) {
		return encodeArray(Subscribe.MESSAGE_TYPE.PMESSAGE.desc(), PSUBSCRIBE_PATTERN, channel, message);
	}

	private static ByteBuf encodeArray(String... parts) {
		Object[] payload = new Object[parts.length];
		for (int i = 0; i < parts.length; i++) {
			payload[i] = new ByteArrayOutputStreamPayload(parts[i]);
		}
		return new ArrayParser(payload).format();
	}

	private static List<RedisClient<?>> snapshot(Set<RedisClient<?>> subscribers) {
		if (subscribers == null || subscribers.isEmpty()) {
			return Collections.emptyList();
		}
		return new ArrayList<>(subscribers);
	}

	@Override
	public String toString() {
		return String.format("KeeperPubSubRegistry[replId=%s, channels=%d, psub=%d]",
				replId, channelSubscribers.size(), patternSubscribers.size());
	}
}

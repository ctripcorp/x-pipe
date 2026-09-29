package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperPubSubConstants;
import com.ctrip.xpipe.redis.keeper.handler.CommandHandlerManager;
import com.ctrip.xpipe.redis.keeper.pubsub.KeeperPubSubRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Phase PS (T-PS.4): SUBSCRIBE / PSUBSCRIBE / UNSUBSCRIBE + CommandHandlerManager 分发（AC-9）。
 */
@RunWith(MockitoJUnitRunner.class)
public class SubscribeCommandHandlerTest extends AbstractRedisKeeperTest {

	@Mock
	private RedisClient subscriber;

	@Mock
	private RedisKeeperServer redisKeeperServer;

	@Mock
	private Channel subscriberChannel;

	private KeeperPubSubRegistry registry;

	private SubscribeCommandHandler subscribeHandler;

	private PsubscribeCommandHandler psubscribeHandler;

	private UnsubscribeCommandHandler unsubscribeHandler;

	@Before
	public void beforeSubscribeCommandHandlerTest() {
		registry = new KeeperPubSubRegistry(ReplId.from(1L));
		subscribeHandler = new SubscribeCommandHandler();
		psubscribeHandler = new PsubscribeCommandHandler();
		unsubscribeHandler = new UnsubscribeCommandHandler();
		when(subscriber.getRedisServer()).thenReturn(redisKeeperServer);
		when(redisKeeperServer.getPubSubRegistry()).thenReturn(registry);
		when(subscriber.channel()).thenReturn(subscriberChannel);
		when(subscriberChannel.isActive()).thenReturn(true);
	}

	@After
	public void afterSubscribeCommandHandlerTest() {
		if (registry != null) {
			registry.close();
		}
	}

	@Test
	public void testSubscribeUpToLimitThenEleventhFails() throws Exception {
		for (int i = 0; i < KeeperPubSubConstants.MAX_SUBSCRIBE_CHANNELS; i++) {
			subscribeHandler.doHandle(new String[]{"ch-" + i}, subscriber);
		}
		Assert.assertEquals(KeeperPubSubConstants.MAX_SUBSCRIBE_CHANNELS, registry.subscribedChannelCount(subscriber));
		Assert.assertTrue(registry.isSubscribed(subscriber, "ch-0"));

		CountDownLatch delivered = new CountDownLatch(1);
		doAnswer(invocation -> {
			delivered.countDown();
			return null;
		}).when(subscriber).sendMessage(any(ByteBuf.class));
		Assert.assertEquals(1, registry.publish("ch-0", "hello"));
		Assert.assertTrue(delivered.await(2, TimeUnit.SECONDS));

		subscribeHandler.doHandle(new String[]{"ch-overflow"}, subscriber);
		Assert.assertFalse(registry.isSubscribed(subscriber, "ch-overflow"));
		Assert.assertEquals(0, registry.publish("ch-overflow", "no"));

		List<String> replies = capturedPayloads(subscriber, KeeperPubSubConstants.MAX_SUBSCRIBE_CHANNELS + 2);
		Assert.assertTrue(replies.get(0).contains("subscribe"));
		Assert.assertTrue(replies.get(0).contains("ch-0"));
		String eleventh = replies.get(KeeperPubSubConstants.MAX_SUBSCRIBE_CHANNELS + 1);
		Assert.assertTrue(eleventh.contains(SubscribeCommandHandler.ERR_CHANNEL_LIMIT));
	}

	@Test
	public void testPsubscribeStarWorksAndOtherPatternFails() throws Exception {
		psubscribeHandler.doHandle(new String[]{KeeperPubSubConstants.PSUBSCRIBE_PATTERN}, subscriber);

		CountDownLatch delivered = new CountDownLatch(1);
		doAnswer(invocation -> {
			delivered.countDown();
			return null;
		}).when(subscriber).sendMessage(any(ByteBuf.class));
		Assert.assertEquals(1, registry.publish("any-ch", "hello"));
		Assert.assertTrue(delivered.await(2, TimeUnit.SECONDS));

		psubscribeHandler.doHandle(new String[]{"delay-*"}, subscriber);

		List<String> replies = capturedPayloads(subscriber, 3);
		Assert.assertTrue(replies.get(0).contains("psubscribe"));
		Assert.assertTrue(replies.get(2).contains(PsubscribeCommandHandler.ERR_PATTERN));
	}

	@Test
	public void testUnsubscribeStopsDelivery() throws Exception {
		subscribeHandler.doHandle(new String[]{"delay"}, subscriber);
		unsubscribeHandler.doHandle(new String[]{"delay"}, subscriber);
		Assert.assertFalse(registry.isSubscribed(subscriber, "delay"));
		Assert.assertEquals(0, registry.publish("delay", "hello"));

		List<String> replies = capturedPayloads(subscriber, 2);
		Assert.assertTrue(replies.get(0).contains("subscribe"));
		Assert.assertTrue(replies.get(1).contains("unsubscribe"));
		Assert.assertTrue(replies.get(1).contains("delay"));
	}

	@Test
	public void testCommandHandlerManagerDispatchesAllThree() throws Exception {
		doAnswer(invocation -> {
			((Runnable) invocation.getArgument(0)).run();
			return null;
		}).when(redisKeeperServer).processCommandSequentially(any());
		CommandHandlerManager manager = new CommandHandlerManager();

		manager.handle(new String[]{"subscribe", "mgr-ch"}, subscriber);
		manager.handle(new String[]{"psubscribe", "*"}, subscriber);
		manager.handle(new String[]{"unsubscribe", "mgr-ch"}, subscriber);

		Assert.assertFalse(registry.isSubscribed(subscriber, "mgr-ch"));
		List<String> replies = capturedPayloads(subscriber, 3);
		Assert.assertTrue(replies.get(0).contains("subscribe"));
		Assert.assertTrue(replies.get(1).contains("psubscribe"));
		Assert.assertTrue(replies.get(2).contains("unsubscribe"));
	}

	private List<String> capturedPayloads(RedisClient client, int expected) {
		ArgumentCaptor<ByteBuf> captor = ArgumentCaptor.forClass(ByteBuf.class);
		verify(client, times(expected)).sendMessage(captor.capture());
		return captor.getAllValues().stream().map(ByteBufUtils::readToString).toList();
	}
}

package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Phase PB (T-PB.4): PUBLISH returns actual delivery count (AC-9).
 */
@RunWith(MockitoJUnitRunner.class)
public class PublishCommandHandlerTest extends AbstractRedisKeeperTest {

	@Mock
	private RedisClient publisher;

	@Mock
	private RedisClient subscriber;

	@Mock
	private RedisKeeperServer redisKeeperServer;

	@Mock
	private Channel subscriberChannel;

	private KeeperPubSubRegistry registry;

	private PublishCommandHandler handler;

	@Before
	public void beforePublishCommandHandlerTest() {
		registry = new KeeperPubSubRegistry(ReplId.from(1L));
		handler = new PublishCommandHandler();
		when(publisher.getRedisServer()).thenReturn(redisKeeperServer);
		when(redisKeeperServer.getPubSubRegistry()).thenReturn(registry);
		when(subscriber.channel()).thenReturn(subscriberChannel);
		when(subscriberChannel.isActive()).thenReturn(true);
	}

	@After
	public void afterPublishCommandHandlerTest() {
		if (registry != null) {
			registry.close();
		}
	}

	@Test
	public void testPublishReturnsActualDeliveryCount() throws Exception {
		CountDownLatch delivered = new CountDownLatch(1);
		doAnswer(invocation -> {
			delivered.countDown();
			return null;
		}).when(subscriber).sendMessage(any(ByteBuf.class));
		registry.subscribe(subscriber, "delay-channel");

		handler.doHandle(new String[]{"delay-channel", "ts-1"}, publisher);

		ArgumentCaptor<ByteBuf> reply = ArgumentCaptor.forClass(ByteBuf.class);
		verify(publisher).sendMessage(reply.capture());
		Assert.assertEquals(":1\r\n", ByteBufUtils.readToString(reply.getValue()));
		Assert.assertTrue(delivered.await(2, TimeUnit.SECONDS));
	}

	@Test
	public void testPublishWithNoSubscriberReturnsZero() {
		handler.doHandle(new String[]{"empty", "msg"}, publisher);

		ArgumentCaptor<ByteBuf> reply = ArgumentCaptor.forClass(ByteBuf.class);
		verify(publisher).sendMessage(reply.capture());
		Assert.assertEquals(":0\r\n", ByteBufUtils.readToString(reply.getValue()));
	}
}

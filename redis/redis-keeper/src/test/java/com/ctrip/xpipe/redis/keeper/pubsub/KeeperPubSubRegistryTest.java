package com.ctrip.xpipe.redis.keeper.pubsub;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Phase PB (T-PB.4): registry + async deliver (AC-9 / AC-10 部分).
 */
@RunWith(MockitoJUnitRunner.class)
public class KeeperPubSubRegistryTest extends AbstractRedisKeeperTest {

	private static final ReplId REPL_ID = ReplId.from(1L);

	private KeeperPubSubRegistry registry;

	@Before
	public void beforeKeeperPubSubRegistryTest() {
		registry = new KeeperPubSubRegistry(REPL_ID, 1);
	}

	@After
	public void afterKeeperPubSubRegistryTest() {
		if (registry != null) {
			registry.close();
		}
	}

	@Test
	public void testPublishReturnsActualReceiverCount() throws Exception {
		RedisClient<?> channelSub = mockClient();
		RedisClient<?> patternSub = mockClient();
		CountDownLatch delivered = new CountDownLatch(2);
		recordDeliver(channelSub, delivered);
		recordDeliver(patternSub, delivered);

		registry.subscribe(channelSub, "ch");
		registry.psubscribeAll(patternSub);

		Assert.assertEquals(2, registry.publish("ch", "hello"));
		Assert.assertTrue(delivered.await(2, TimeUnit.SECONDS));

		String channelPayload = lastPayload(channelSub);
		Assert.assertTrue(channelPayload.contains("message"));
		Assert.assertTrue(channelPayload.contains("ch"));
		Assert.assertTrue(channelPayload.contains("hello"));

		String patternPayload = lastPayload(patternSub);
		Assert.assertTrue(patternPayload.contains("pmessage"));
		Assert.assertTrue(patternPayload.contains("hello"));
	}

	@Test
	public void testClientDisconnectRemovesFromRegistry() throws Exception {
		RedisClient<?> client = mockClient();
		AtomicReference<Releasable> hook = new AtomicReference<>();
		doAnswer(invocation -> {
			hook.set(invocation.getArgument(0));
			return null;
		}).when(client).addChannelCloseReleaseResources(any());

		registry.subscribe(client, "ch");
		Assert.assertNotNull(hook.get());
		hook.get().release();

		Assert.assertEquals(0, registry.publish("ch", "hello"));
		verify(client, never()).sendMessage(any(ByteBuf.class));
	}

	@Test
	public void testImmediateCloseHookDoesNotLeaveZombie() throws Exception {
		RedisClient<?> client = mockClient();
		doAnswer(invocation -> {
			Releasable hook = invocation.getArgument(0);
			hook.release();
			return null;
		}).when(client).addChannelCloseReleaseResources(any());

		registry.subscribe(client, "ch");
		Assert.assertEquals(0, registry.publish("ch", "hello"));
		verify(client, never()).sendMessage(any(ByteBuf.class));
	}

	@Test
	public void testSubscribeInactiveChannelDoesNotLeaveZombie() {
		RedisClient<?> client = mockClient();
		Channel channel = mock(Channel.class);
		when(client.channel()).thenReturn(channel);
		when(channel.isActive()).thenReturn(false);

		registry.subscribe(client, "ch");
		registry.psubscribeAll(client);
		Assert.assertEquals(0, registry.publish("ch", "hello"));
		verify(client, never()).addChannelCloseReleaseResources(any());
		verify(client, never()).sendMessage(any(ByteBuf.class));
	}

	@Test
	public void testQueueFullDropsAndDoesNotBlockOrThrow() throws Exception {
		RedisClient<?> client = mockClient();
		CountDownLatch entered = new CountDownLatch(1);
		CountDownLatch block = new CountDownLatch(1);
		doAnswer(invocation -> {
			entered.countDown();
			Assert.assertTrue(block.await(5, TimeUnit.SECONDS));
			return null;
		}).when(client).sendMessage(any(ByteBuf.class));
		registry.subscribe(client, "ch");

		Assert.assertEquals(1, registry.publish("ch", "first"));
		Assert.assertTrue(entered.await(2, TimeUnit.SECONDS));
		Assert.assertEquals(1, registry.publish("ch", "queued"));

		long start = System.nanoTime();
		int dropped = registry.publish("ch", "overflow");
		long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
		Assert.assertEquals(0, dropped);
		Assert.assertTrue("publish must not block on full queue, elapsed=" + elapsedMs, elapsedMs < 500);

		block.countDown();
	}

	@Test
	public void testFanoutGivesEachClientIndependentBuf() throws Exception {
		RedisClient<?> first = mockClient();
		RedisClient<?> second = mockClient();
		RedisClient<?> third = mockClient();
		CountDownLatch delivered = new CountDownLatch(3);
		recordDeliver(first, delivered);
		recordDeliver(second, delivered);
		recordDeliver(third, delivered);

		registry.subscribe(first, "ch");
		registry.subscribe(second, "ch");
		registry.subscribe(third, "ch");

		Assert.assertEquals(3, registry.publish("ch", "hello"));
		Assert.assertTrue(delivered.await(2, TimeUnit.SECONDS));

		ByteBuf firstBuf = capturedPayload(first);
		ByteBuf secondBuf = capturedPayload(second);
		ByteBuf thirdBuf = capturedPayload(third);
		Assert.assertNotSame(firstBuf, secondBuf);
		Assert.assertNotSame(secondBuf, thirdBuf);
		String firstPayload = ByteBufUtils.readToString(firstBuf);
		Assert.assertTrue("consuming one copy must not empty the others", secondBuf.readableBytes() > 0);
		Assert.assertEquals(firstPayload, ByteBufUtils.readToString(secondBuf));
		Assert.assertEquals(firstPayload, ByteBufUtils.readToString(thirdBuf));
		Assert.assertTrue(firstPayload.contains("hello"));
	}

	@Test
	public void testFanoutSendFailureDoesNotBlockOthers() throws Exception {
		RedisClient<?> failing = mockClient();
		RedisClient<?> ok = mockClient();
		CountDownLatch delivered = new CountDownLatch(1);
		doAnswer(invocation -> {
			throw new IllegalStateException("channel closed");
		}).when(failing).sendMessage(any(ByteBuf.class));
		recordDeliver(ok, delivered);

		registry.subscribe(failing, "ch");
		registry.subscribe(ok, "ch");

		Assert.assertEquals(2, registry.publish("ch", "hello"));
		Assert.assertTrue(delivered.await(2, TimeUnit.SECONDS));
		String payload = lastPayload(ok);
		Assert.assertTrue(payload.contains("hello"));
	}

	@Test
	public void testDeliverNotOnReplicationIoThread() throws Exception {
		RedisClient<?> client = mockClient();
		CountDownLatch delivered = new CountDownLatch(1);
		AtomicReference<String> deliverThread = new AtomicReference<>();
		doAnswer(invocation -> {
			deliverThread.set(Thread.currentThread().getName());
			delivered.countDown();
			return null;
		}).when(client).sendMessage(any(ByteBuf.class));
		registry.subscribe(client, "ch");

		AtomicReference<Integer> published = new AtomicReference<>();
		Thread replIo = new Thread(() -> published.set(registry.publish("ch", "hello")), "repl-io-sim");
		replIo.start();
		replIo.join(2000);
		Assert.assertFalse(replIo.isAlive());
		Assert.assertEquals(Integer.valueOf(1), published.get());
		Assert.assertTrue(delivered.await(2, TimeUnit.SECONDS));
		Assert.assertNotNull(deliverThread.get());
		Assert.assertFalse(deliverThread.get().contains("repl-io"));
		Assert.assertTrue(deliverThread.get().contains("pubsub-deliver"));
	}

	@SuppressWarnings("unchecked")
	private RedisClient<?> mockClient() {
		RedisClient<?> client = mock(RedisClient.class);
		Channel channel = mock(Channel.class);
		when(client.channel()).thenReturn(channel);
		when(channel.isActive()).thenReturn(true);
		return client;
	}

	private void recordDeliver(RedisClient<?> client, CountDownLatch delivered) {
		doAnswer(invocation -> {
			delivered.countDown();
			return null;
		}).when(client).sendMessage(any(ByteBuf.class));
	}

	private String lastPayload(RedisClient<?> client) {
		return ByteBufUtils.readToString(capturedPayload(client));
	}

	private ByteBuf capturedPayload(RedisClient<?> client) {
		ArgumentCaptor<ByteBuf> captor = ArgumentCaptor.forClass(ByteBuf.class);
		verify(client, atLeastOnce()).sendMessage(captor.capture());
		return captor.getValue();
	}
}

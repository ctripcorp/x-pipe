package com.ctrip.xpipe.redis.keeper.pubsub;

import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Phase PH (T-PH.4 ①④⑤): ACTIVE/BACKUP appendCommands 钩子。AC-10 / AC-21。
 */
@RunWith(MockitoJUnitRunner.class)
public class KeeperPubSubParseHookTest extends AbstractRedisKeeperTest {

	private static final String REPL_ID = "000000000000000000000000000000000000000A";

	private TestKeeperConfig keeperConfig;

	private AsyncFileSystem fs;

	private DefaultReplicationStoreManager manager;

	@Before
	public void beforeKeeperPubSubParseHookTest() {
		keeperConfig = new TestKeeperConfig();
		keeperConfig.setReplicationStoreGcIntervalSeconds(60);
		keeperConfig.setMinTimeMilliToGcAfterCreate(60_000);
	}

	@After
	public void afterKeeperPubSubParseHookTest() {
		stopDispose(manager);
		if (fs != null) {
			fs.shutdown();
		}
	}

	@Test
	public void testManagerBindsHookOnWritableCreateAndSkipsReadOnly() throws Exception {
		List<String> received = new ArrayList<>();
		KeeperPubSubParseHook hook = new KeeperPubSubParseHook(createRedisOpParser(),
				(channel, message) -> received.add(channel + ":" + message));
		manager = new DefaultReplicationStoreManager(
				keeperConfig, getReplId(), randomKeeperRunid(), new File(getTestFileDir()),
				createkeeperMonitor(), mock(SyncRateManager.class), createRedisOpParser(), null,
				fs = createTestAsyncFileSystem());
		manager.setPubSubParseHook(hook);
		LifecycleHelper.initializeIfPossible(manager);
		LifecycleHelper.startIfPossible(manager);

		DefaultReplicationStore writable = (DefaultReplicationStore) manager.create();
		writable.psyncContinueFrom(REPL_ID, 1);
		Assert.assertNotNull(writable.getPubSubParseHook());
		int wrote = writable.appendCommands(publishBuf("ch", "from-manager"));
		Assert.assertTrue(wrote > 0);
		Assert.assertEquals(Collections.singletonList("ch:from-manager"), received);

		LifecycleHelper.stopIfPossible(manager);
		manager.setReadOnly(true);
		LifecycleHelper.startIfPossible(manager);
		DefaultReplicationStore readOnly = (DefaultReplicationStore) manager.getCurrent();
		Assert.assertNotNull(readOnly);
		Assert.assertNull(readOnly.getPubSubParseHook());
	}

	@Test
	public void testPsyncAppendPublishReachesSubscriber() throws Exception {
		DefaultReplicationStore store = openWritableStore();
		KeeperPubSubRegistry registry = new KeeperPubSubRegistry(ReplId.from(1L), 8);
		try {
			RedisClient<?> client = mockClient();
			CountDownLatch delivered = new CountDownLatch(1);
			doAnswer(invocation -> {
				delivered.countDown();
				return null;
			}).when(client).sendMessage(any(ByteBuf.class));
			registry.subscribe(client, "ch");

			store.setPubSubParseHook(new KeeperPubSubParseHook(createRedisOpParser(), registry::publish));
			store.appendCommands(publishBuf("ch", "hello"));

			Assert.assertTrue(delivered.await(2, TimeUnit.SECONDS));
			String payload = ByteBufUtils.readToString(capturedPayload(client));
			Assert.assertTrue(payload.contains("message"));
			Assert.assertTrue(payload.contains("ch"));
			Assert.assertTrue(payload.contains("hello"));
		} finally {
			registry.close();
		}
	}

	@Test
	public void testNonPublishIsIgnoredAndParseErrorDoesNotFailWrite() throws Exception {
		DefaultReplicationStore store = openWritableStore();
		List<String> received = new ArrayList<>();
		store.setPubSubParseHook(new KeeperPubSubParseHook(createRedisOpParser(),
				(channel, message) -> received.add(channel + ":" + message)));

		int setWrote = store.appendCommands(setBuf("k1", "v1"));
		Assert.assertTrue(setWrote > 0);
		Assert.assertTrue(received.isEmpty());

		int dirtyWrote = store.appendCommands(Unpooled.wrappedBuffer("xxx*not-a-cmd".getBytes(StandardCharsets.US_ASCII)));
		Assert.assertTrue(dirtyWrote > 0);

		store.setPubSubParseHook(new KeeperPubSubParseHook(createRedisOpParser(), (channel, message) -> {
			received.add(channel + ":" + message);
			throw new IllegalStateException("inject-parse-listener");
		}));
		int boomWrote = store.appendCommands(publishBuf("ch", "boom"));
		Assert.assertTrue(boomWrote > 0);
		Assert.assertEquals(1, received.size());
		Assert.assertEquals("ch:boom", received.get(0));

		int afterWrote = store.appendCommands(publishBuf("ch", "after"));
		Assert.assertTrue(afterWrote > 0);
		Assert.assertEquals(2, received.size());
		Assert.assertEquals("ch:after", received.get(1));
	}

	@Test
	public void testAppendDoesNotWaitForSubscriberWrite() throws Exception {
		DefaultReplicationStore store = openWritableStore();
		KeeperPubSubRegistry registry = new KeeperPubSubRegistry(ReplId.from(1L), 8);
		try {
			CountDownLatch entered = new CountDownLatch(1);
			CountDownLatch release = new CountDownLatch(1);
			RedisClient<?> client = mockClient();
			doAnswer(invocation -> {
				entered.countDown();
				Assert.assertTrue(release.await(3, TimeUnit.SECONDS));
				return null;
			}).when(client).sendMessage(any(ByteBuf.class));
			registry.subscribe(client, "ch");
			store.setPubSubParseHook(new KeeperPubSubParseHook(createRedisOpParser(), registry::publish));

			long begin = System.currentTimeMillis();
			int wrote = store.appendCommands(publishBuf("ch", "hello"));
			long elapsed = System.currentTimeMillis() - begin;
			Assert.assertTrue(wrote > 0);
			Assert.assertTrue("appendCommands blocked on subscriber write, elapsed=" + elapsed, elapsed < 500);
			release.countDown();
			Assert.assertTrue(entered.await(2, TimeUnit.SECONDS));
		} finally {
			registry.close();
		}
	}

	@Test
	public void testHookOnCallerThreadNotDeliverThread() throws Exception {
		List<String> parseThreads = new ArrayList<>();
		KeeperPubSubParseHook hook = new KeeperPubSubParseHook(createRedisOpParser(), (channel, message) ->
				parseThreads.add(Thread.currentThread().getName()));
		AtomicReference<String> caller = new AtomicReference<>();
		Thread t = new Thread(() -> {
			caller.set(Thread.currentThread().getName());
			hook.onCommands(publishBuf("ch", "hello"));
		}, "repl-io-sim");
		t.start();
		t.join(2000);
		Assert.assertFalse(t.isAlive());
		Assert.assertEquals(1, parseThreads.size());
		Assert.assertEquals(caller.get(), parseThreads.get(0));
		Assert.assertTrue(parseThreads.get(0).contains("repl-io"));
	}

	private DefaultReplicationStore openWritableStore() throws Exception {
		fs = createTestAsyncFileSystem();
		manager = new DefaultReplicationStoreManager(
				keeperConfig, getReplId(), randomKeeperRunid(), new File(getTestFileDir()),
				createkeeperMonitor(), mock(SyncRateManager.class), createRedisOpParser(), null, fs);
		LifecycleHelper.initializeIfPossible(manager);
		LifecycleHelper.startIfPossible(manager);
		DefaultReplicationStore store = (DefaultReplicationStore) manager.create();
		store.psyncContinueFrom(REPL_ID, 1);
		return store;
	}

	private static ByteBuf publishBuf(String channel, String message) {
		return resp(new String[]{"PUBLISH", channel, message});
	}

	private static ByteBuf setBuf(String key, String value) {
		return resp(new String[]{"SET", key, value});
	}

	private static ByteBuf resp(String[] parts) {
		StringBuilder sb = new StringBuilder();
		sb.append('*').append(parts.length).append("\r\n");
		for (String part : parts) {
			sb.append('$').append(part.length()).append("\r\n").append(part).append("\r\n");
		}
		return Unpooled.wrappedBuffer(sb.toString().getBytes(StandardCharsets.US_ASCII));
	}

	@SuppressWarnings("unchecked")
	private RedisClient<?> mockClient() {
		RedisClient<?> client = mock(RedisClient.class);
		Channel channel = mock(Channel.class);
		when(client.channel()).thenReturn(channel);
		when(channel.isActive()).thenReturn(true);
		return client;
	}

	private ByteBuf capturedPayload(RedisClient<?> client) {
		ArgumentCaptor<ByteBuf> captor = ArgumentCaptor.forClass(ByteBuf.class);
		verify(client, atLeastOnce()).sendMessage(captor.capture());
		return captor.getValue();
	}

	private static void stopDispose(DefaultReplicationStoreManager toStop) {
		if (toStop == null) {
			return;
		}
		try {
			LifecycleHelper.stopIfPossible(toStop);
		} catch (Throwable ignore) {
		}
		try {
			LifecycleHelper.disposeIfPossible(toStop);
		} catch (Throwable ignore) {
		}
	}
}

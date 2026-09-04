package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperContextTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.handler.keeper.KeeperCommandHandler;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.store.readonly.ReadOnlyCommandStore;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Phase LC (T-LC.5): PREPARE host lifecycle and getCurrentReplicationStore gate. AC-1 / AC-2b / AC-4 / AC-21.
 */
public class DefaultRedisKeeperServerPrepareWatchTest extends AbstractRedisKeeperContextTest {

	private static final int PREPARE_ACTIVE_ROUNDS = 3;

	private final KeeperCommandHandler keeperCommandHandler = new KeeperCommandHandler();

	@Test
	public void testWatchDisabledKeepsPrepareGateAndStoppedManager() throws Exception {
		DefaultRedisKeeperServer server = startActiveServer(watchConfig(false), false);
		try {
			becomePrepare(server);
			assertPrepareGateClosed(server);
			Assert.assertNull(server.getPrepareWatcher());
			Assert.assertEquals(0, countPrepareWatchThreads());
		} finally {
			stopQuietly(server);
		}
	}

	@Test
	public void testWatchEnabledButNotTfsSkipsReadOnly() throws Exception {
		DefaultRedisKeeperServer server = startActiveServer(watchConfig(true), false);
		try {
			becomePrepare(server);
			assertPrepareGateClosed(server);
			Assert.assertNull(server.getPrepareWatcher());
			Assert.assertEquals(0, countPrepareWatchThreads());
		} finally {
			stopQuietly(server);
		}
	}

	@Test
	public void testReadOnlyOpensExistingLatestWithoutCreate() throws Exception {
		DefaultRedisKeeperServer server = startActiveServer(watchConfig(true), true);
		try {
			ReplicationStore storeBefore = server.getReplicationStore();
			File dirBefore = ((DefaultReplicationStore) storeBefore).getBaseDir();
			DefaultReplicationStoreManager manager =
					spy((DefaultReplicationStoreManager) server.getReplicationStoreManager());
			server.setReplicationStoreManager(manager);
			clearInvocations(manager);

			becomePrepare(server);

			Assert.assertTrue(manager.isReadOnly());
			Assert.assertTrue(manager.getLifecycleState().isStarted());
			Assert.assertNotNull(server.getPrepareWatcher());
			verify(manager, never()).create();

			ReplicationStore opened = server.getReplicationStore();
			Assert.assertNotNull(opened);
			Assert.assertEquals(dirBefore, ((DefaultReplicationStore) opened).getBaseDir());
			Assert.assertEquals(dirBefore.getName(), manager.reloadLatestStoreDir());
			Assert.assertSame(opened, manager.getOpenedStore());
			CommandStore cmdStore = ((DefaultReplicationStore) opened).getCommandStore();
			if (cmdStore != null) {
				Assert.assertTrue("read-only open must use ReadOnlyCommandStore, got " + cmdStore.getClass(),
						cmdStore instanceof ReadOnlyCommandStore);
			}
			verify(manager, never()).create();
		} finally {
			stopQuietly(server);
		}
	}

	@Test
	public void testRepeatedPrepareActiveDoesNotAccumulateWatchThreads() throws Exception {
		DefaultRedisKeeperServer server = startActiveServer(watchConfig(true), true);
		DefaultEndPoint master = new DefaultEndPoint("127.0.0.1", 0);
		try {
			for (int i = 0; i < PREPARE_ACTIVE_ROUNDS; i++) {
				becomePrepare(server);
				Assert.assertEquals(KeeperState.PREPARE, server.getRedisKeeperServerState().keeperState());
				Assert.assertTrue(server.getReplicationStoreManager().isReadOnly());
				Assert.assertNotNull(server.getPrepareWatcher());
				waitConditionUntilTimeOut(() -> countPrepareWatchThreads() >= 1);

				server.getRedisKeeperServerState().becomeActive(master);
				Assert.assertEquals(KeeperState.ACTIVE, server.getRedisKeeperServerState().keeperState());
				Assert.assertFalse(server.getReplicationStoreManager().isReadOnly());
				Assert.assertNull(server.getPrepareWatcher());
				Assert.assertTrue(server.getReplicationStoreManager().getLifecycleState().isStarted());
				waitConditionUntilTimeOut(() -> countPrepareWatchThreads() == 0);
				Assert.assertNotNull(server.getReplicationStore());
			}
			waitConditionUntilTimeOut(() -> countPrepareWatchThreads() == 0);
		} finally {
			stopQuietly(server);
		}
	}

	@Test
	public void testReadOnlyStartFailureDoesNotChangeSetStatePrepareReply() throws Exception {
		DefaultRedisKeeperServer server = startActiveServer(watchConfig(true), true);
		try {
			DefaultReplicationStoreManager manager =
					spy((DefaultReplicationStoreManager) server.getReplicationStoreManager());
			doThrow(new RuntimeException("inject-readonly")).when(manager).setReadOnly(true);
			server.setReplicationStoreManager(manager);

			String resp = invokeKeeperCommand(server, "setstate", "PREPARE", "127.0.0.1", "6379");
			Assert.assertEquals("+" + RedisProtocol.OK + "\r\n", resp);
			Assert.assertEquals(KeeperState.PREPARE, server.getRedisKeeperServerState().keeperState());
			Assert.assertFalse(manager.isReadOnly());
			Assert.assertNull(server.getPrepareWatcher());
			try {
				server.getReplicationStore();
				Assert.fail("PREPARE without read-only must still refuse getReplicationStore");
			} catch (RedisKeeperServerStateException expected) {
				Assert.assertTrue(expected.getMessage().contains("PREPARE"));
			}
		} finally {
			stopQuietly(server);
		}
	}

	private DefaultRedisKeeperServer startActiveServer(TestKeeperConfig config, boolean tfsMode) throws Exception {
		DefaultRedisKeeperServer server = (DefaultRedisKeeperServer) createRedisKeeperServer(config, tfsMode);
		server.initialize();
		server.start();
		server.setRedisKeeperServerState(
				new RedisKeeperServerStateActive(server, new DefaultEndPoint("127.0.0.1", 0)));
		Assert.assertTrue(server.getReplicationStore().checkOk());
		return server;
	}

	private TestKeeperConfig watchConfig(boolean enabled) {
		TestKeeperConfig config = new TestKeeperConfig();
		config.setPrepareStoreWatchEnabled(enabled);
		config.setPrepareWatchMetaIntervalMilli(50);
		return config;
	}

	private void becomePrepare(DefaultRedisKeeperServer server) {
		server.getRedisKeeperServerState().becomePrepare(new DefaultEndPoint("127.0.0.1", randomPort()));
	}

	private void assertPrepareGateClosed(DefaultRedisKeeperServer server) throws Exception {
		Assert.assertEquals(KeeperState.PREPARE, server.getRedisKeeperServerState().keeperState());
		Assert.assertFalse(server.getReplicationStoreManager().isReadOnly());
		Assert.assertTrue(server.getReplicationStoreManager().getLifecycleState().isPositivelyStopped());
		try {
			server.getReplicationStore();
			Assert.fail("PREPARE must refuse getReplicationStore");
		} catch (RedisKeeperServerStateException expected) {
			Assert.assertTrue(expected.getMessage().contains("PREPARE"));
		}
	}

	private String invokeKeeperCommand(RedisKeeperServer server, String... args) throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel();
		DefaultRedisClient client = new DefaultRedisClient(channel, server);
		keeperCommandHandler.handle(args, client);
		Object outbound = channel.readOutbound();
		Assert.assertNotNull(outbound);
		Assert.assertTrue(outbound instanceof ByteBuf);
		ByteBuf buf = (ByteBuf) outbound;
		try {
			return ByteBufUtils.readToString(buf.duplicate());
		} finally {
			buf.release();
		}
	}

	private void stopQuietly(DefaultRedisKeeperServer server) {
		try {
			server.stop();
		} catch (Exception e) {
			logger.info("[stopQuietly] stop", e);
		}
		try {
			server.dispose();
		} catch (Exception e) {
			logger.info("[stopQuietly] dispose", e);
		}
	}

	private static long countPrepareWatchThreads() {
		return Thread.getAllStackTraces().keySet().stream()
				.filter(t -> t.getName() != null && t.getName().contains("prepare-watch"))
				.count();
	}

	@Override
	protected String getXpipeMetaConfigFile() {
		return "keeper-test.xml";
	}
}

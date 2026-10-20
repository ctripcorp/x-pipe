package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.protocal.cmd.InMemoryGapAllowedSync;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperContextTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.handler.keeper.KeeperCommandHandler;
import com.ctrip.xpipe.redis.keeper.prepare.PrepareCmdParser;
import com.ctrip.xpipe.redis.keeper.prepare.PrepareStoreWatcher;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.store.readonly.ReadOnlyCommandStore;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import org.springframework.test.util.ReflectionTestUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Phase LC (T-LC.5): PREPARE host lifecycle and getCurrentReplicationStore gate. AC-1 / AC-2b / AC-4 / AC-21.
 */
public class DefaultRedisKeeperServerPrepareWatchTest extends AbstractRedisKeeperContextTest {

	private static final int PREPARE_ACTIVE_ROUNDS = 3;

	private static final String PREPARE_REPL_ID = "000000000000000000000000000000000000000A";

	private static final byte[] PING = "*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII);

	private static final int WATCH_CYCLE_WAIT_MILLI = 6 * PrepareStoreWatcher.WATCH_TICK_MILLI;

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
			Assert.assertNull(server.getPrepareCmdParser());
			verify(manager, never()).create();

			Assert.assertNull("get semantics must not open the read-only store", server.getReplicationStore());
			// request-side data path (read-only createIfNotExist degrades to getCurrent)
			ReplicationStore opened = server.getOrCreateReplicationStore();
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
	public void testPubsubParseStartsParserOnPrepareAndStopsOnLeave() throws Exception {
		TestKeeperConfig config = watchConfig(true);
		config.setPubsubParseEnabled(true);
		DefaultRedisKeeperServer server = startActiveServer(config, true);
		DefaultEndPoint master = new DefaultEndPoint("127.0.0.1", 0);
		try {
			Assert.assertNull(server.getPrepareCmdParser());
			becomePrepare(server);
			Assert.assertNotNull(server.getPrepareCmdParser());
			Assert.assertTrue(server.getPrepareCmdParser().isRunning());
			waitConditionUntilTimeOut(() -> countPrepareCmdParserThreads() >= 1);

			server.getRedisKeeperServerState().becomeActive(master);
			Assert.assertNull(server.getPrepareCmdParser());
			waitConditionUntilTimeOut(() -> countPrepareCmdParserThreads() == 0);
		} finally {
			stopQuietly(server);
		}
	}

	@Test
	public void testFullSyncToSlaveRefusedInPrepare() throws Exception {
		DefaultRedisKeeperServer server = startActiveServer(watchConfig(true), true);
		try {
			becomePrepare(server);
			RedisSlave slave = mock(RedisSlave.class);
			when(slave.isOpen()).thenReturn(true);
			server.fullSyncToSlave(slave, false);
			verify(slave).close();
			Assert.assertNull(server.rdbDumper());
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
				server.getOrCreateReplicationStore();
				Assert.fail("PREPARE without read-only must still refuse getOrCreateReplicationStore");
			} catch (RedisKeeperServerStateException expected) {
				Assert.assertTrue(expected.getMessage().contains("PREPARE"));
			}
			Assert.assertNull(server.getReplicationStore());
		} finally {
			stopQuietly(server);
		}
	}

	@Test
	public void testDeferredWriteHandleRepliesErrorAndStillStartsWatch() throws Exception {
		DefaultRedisKeeperServer server = startActiveServer(watchConfig(true), true);
		try {
			DefaultReplicationStoreManager manager = spyWithDeferredRelease(server);

			String resp = invokeKeeperCommand(server, "setstate", "PREPARE", "127.0.0.1", "6379");
			Assert.assertTrue(resp, resp.startsWith("-"));
			Assert.assertEquals(KeeperState.PREPARE, server.getRedisKeeperServerState().keeperState());
			Assert.assertNull("production store released", manager.getOpenedStore());
			Assert.assertTrue(manager.isReadOnly());
			Assert.assertTrue(manager.getLifecycleState().isStarted());
			Assert.assertNotNull(server.getPrepareWatcher());
		} finally {
			stopQuietly(server);
		}
	}

	@Test
	public void testDeferredWriteHandleWithWatchDisabledRepliesErrorAndStopsManager() throws Exception {
		DefaultRedisKeeperServer server = startActiveServer(watchConfig(false), true);
		try {
			spyWithDeferredRelease(server);

			String resp = invokeKeeperCommand(server, "setstate", "PREPARE", "127.0.0.1", "6379");
			Assert.assertTrue(resp, resp.startsWith("-"));
			assertPrepareGateClosed(server);
		} finally {
			stopQuietly(server);
		}
	}

	@Test
	public void testLeaveWatchReleaseFailureDoesNotFailNextPrepare() throws Exception {
		DefaultRedisKeeperServer server = startActiveServer(watchConfig(true), true);
		try {
			DefaultReplicationStoreManager manager =
					spy((DefaultReplicationStoreManager) server.getReplicationStoreManager());
			server.setReplicationStoreManager(manager);
			becomePrepare(server);
			doAnswer(inv -> {
				inv.callRealMethod();
				return false;
			}).when(manager).releaseCurrentStore(false);
			server.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("127.0.0.1", 0));
			Assert.assertEquals(KeeperState.ACTIVE, server.getRedisKeeperServerState().keeperState());
			doCallRealMethod().when(manager).releaseCurrentStore(false);

			String resp = invokeKeeperCommand(server, "setstate", "PREPARE", "127.0.0.1", "6379");
			Assert.assertEquals("+" + RedisProtocol.OK + "\r\n", resp);
			Assert.assertNotNull(server.getPrepareWatcher());
		} finally {
			stopQuietly(server);
		}
	}

	/** T-P7.2: reopen failure ⇒ Redis ERROR, Manager rolled back to STOPPED so GC cannot reopen; next SETSTATE succeeds. */
	@Test
	public void testReenterFailureStopsManagerAndNextSetStateSucceeds() throws Exception {
		DefaultRedisKeeperServer server = startActiveServer(watchConfig(true), true);
		try {
			becomePrepare(server);
			DefaultReplicationStoreManager manager =
					spy((DefaultReplicationStoreManager) server.getReplicationStoreManager());
			server.setReplicationStoreManager(manager);
			doThrow(new IllegalStateException("writer already open")).when(manager).createIfNotExist();

			String resp = invokeKeeperCommand(server, "setstate", "ACTIVE", "127.0.0.1", "6379");

			Assert.assertTrue(resp, resp.startsWith("-"));
			Assert.assertTrue(resp, resp.contains("writer already open"));
			Assert.assertEquals(KeeperState.PREPARE, server.getRedisKeeperServerState().keeperState());
			Assert.assertTrue("reenter failure must roll back the Manager start",
					manager.getLifecycleState().isPositivelyStopped());
			Assert.assertNull(manager.getOpenedStore());
			Assert.assertNull("gc getCurrent must not reopen a writable store under PREPARE", manager.getCurrent());
			Assert.assertNull(server.getPrepareWatcher());

			doCallRealMethod().when(manager).createIfNotExist();
			String retry = invokeKeeperCommand(server, "setstate", "ACTIVE", "127.0.0.1", "6379");

			Assert.assertEquals("+" + RedisProtocol.OK + "\r\n", retry);
			Assert.assertEquals(KeeperState.ACTIVE, server.getRedisKeeperServerState().keeperState());
			Assert.assertTrue(manager.getLifecycleState().isStarted());
			Assert.assertNotNull(server.getReplicationStore());
		} finally {
			stopQuietly(server);
		}
	}

	/**
	 * T-P7.2: {@code start} failure makes the rollback a no-op (lifecycle already rolled the phase back), and it
	 * need not run — {@code leavePrepareWatch} stopped the Manager first, so reopen stays refused.
	 */
	@Test
	public void testReenterStartFailureLeavesManagerStoppedWithoutRollback() throws Exception {
		DefaultRedisKeeperServer server = startActiveServer(watchConfig(true), true);
		try {
			becomePrepare(server);
			DefaultReplicationStoreManager manager =
					spy((DefaultReplicationStoreManager) server.getReplicationStoreManager());
			server.setReplicationStoreManager(manager);
			doThrow(new RuntimeException("inject-start")).when(manager).start();

			String resp = invokeKeeperCommand(server, "setstate", "ACTIVE", "127.0.0.1", "6379");

			Assert.assertTrue(resp, resp.startsWith("-"));
			Assert.assertEquals(KeeperState.PREPARE, server.getRedisKeeperServerState().keeperState());
			Assert.assertTrue(manager.getLifecycleState().isPositivelyStopped());
			Assert.assertNull("stopped Manager must refuse reopen", manager.getCurrent());
			verify(manager, never()).createIfNotExist();
		} finally {
			stopQuietly(server);
		}
	}

	/** T-P7.2: the rollback is skipped once the state switch already happened — an ACTIVE keeper keeps its Manager. */
	@Test
	public void testReenterFailureAfterStateSwitchKeepsManagerStarted() throws Exception {
		DefaultRedisKeeperServer server = startActiveServer(watchConfig(true), true);
		try {
			becomePrepare(server);
			DefaultReplicationStoreManager manager =
					spy((DefaultReplicationStoreManager) server.getReplicationStoreManager());
			server.setReplicationStoreManager(manager);
			doThrow(new IllegalStateException("inject-after-switch")).when(manager).createIfNotExist();
			server.setRedisKeeperServerState(
					new RedisKeeperServerStateActive(server, new DefaultEndPoint("127.0.0.1", 0)));

			try {
				server.doReenterFromPrepare(new DefaultEndPoint("127.0.0.1", 0), true);
				Assert.fail("reopen must fail");
			} catch (IllegalStateException expected) {
				Assert.assertEquals("inject-after-switch", expected.getMessage());
			}

			Assert.assertEquals(KeeperState.ACTIVE, server.getRedisKeeperServerState().keeperState());
			Assert.assertTrue("rollback must not stop the Manager of an ACTIVE keeper",
					manager.getLifecycleState().isStarted());
		} finally {
			stopQuietly(server);
		}
	}

	/** keeper-tfs-m6 T-P12.6 */
	@Test
	public void testPrepareOnFreshStoreServesCmdAfterInPlaceBuild() throws Exception {
		TestKeeperConfig config = watchConfig(true);
		config.setPubsubParseEnabled(true);
		DefaultRedisKeeperServer server = startActiveServer(config, true);
		DefaultReplicationStoreManager occupying = null;
		try {
			DefaultReplicationStore activeStore = (DefaultReplicationStore) server.getReplicationStore();
			Assert.assertTrue(activeStore.isFresh());
			File dir = activeStore.getBaseDir();
			becomePrepare(server);
			DefaultReplicationStoreManager manager =
					(DefaultReplicationStoreManager) server.getReplicationStoreManager();
			waitConditionUntilTimeOut(() -> manager.getOpenedStore() != null, WATCH_CYCLE_WAIT_MILLI);
			DefaultReplicationStore opened = (DefaultReplicationStore) manager.getOpenedStore();
			Assert.assertEquals(dir, opened.getBaseDir());
			Assert.assertNull(opened.getCommandStore());
			Assert.assertTrue(opened.isFresh());

			occupying = new DefaultReplicationStoreManager(config, manager.getReplId(), server.getKeeperRunid(),
					manager.getBaseDir().getParentFile(), createkeeperMonitor(), mock(SyncRateManager.class),
					createRedisOpParser(), null, asyncFileSystem());
			LifecycleHelper.initializeIfPossible(occupying);
			LifecycleHelper.startIfPossible(occupying);
			Assert.assertEquals(manager.getBaseDir(), occupying.getBaseDir());
			DefaultReplicationStore writable = (DefaultReplicationStore) occupying.getCurrent();
			Assert.assertNotNull("occupying must reopen the latest store " + manager.getCurrentMetaCache(), writable);
			Assert.assertEquals(dir, writable.getBaseDir());
			writable.psyncContinueFrom(PREPARE_REPL_ID, 1);
			writable.appendCommands(Unpooled.wrappedBuffer(PING));

			waitConditionUntilTimeOut(() -> opened.getCommandStore() != null && !opened.isFresh(),
					WATCH_CYCLE_WAIT_MILLI);
			Assert.assertSame("built in place, no store switch", opened, manager.getOpenedStore());
			PrepareCmdParser parser = server.getPrepareCmdParser();
			waitConditionUntilTimeOut(() -> parser.getAttachCount() >= 1, WATCH_CYCLE_WAIT_MILLI);

			InMemoryGapAllowedSync slaveSync = new InMemoryGapAllowedSync("127.0.0.1", server.getListeningPort(),
					PREPARE_REPL_ID, 1, scheduled);
			slaveSync.execute();
			waitConditionUntilTimeOut(() -> slaveSync.getCommands().length >= PING.length, WATCH_CYCLE_WAIT_MILLI);
			Assert.assertEquals(0, slaveSync.getFullSyncCnt());
			Assert.assertEquals(1, server.slaves().size());

			writable.appendCommands(Unpooled.wrappedBuffer(PING));
			waitConditionUntilTimeOut(() -> slaveSync.getCommands().length >= 2 * PING.length,
					WATCH_CYCLE_WAIT_MILLI);
			Assert.assertArrayEquals(concat(PING, PING), slaveSync.getCommands());
			Assert.assertEquals("slave must not be disconnected", 1, server.slaves().size());
			Assert.assertFalse(slaveSync.future().isDone());
			Assert.assertSame(opened, manager.getOpenedStore());
			Assert.assertEquals(1, parser.getAttachCount());
		} finally {
			stopQuietly(server);
			if (occupying != null) {
				LifecycleHelper.stopIfPossible(occupying);
				LifecycleHelper.disposeIfPossible(occupying);
			}
		}
	}

	/** keeper-tfs-m6 T-P13.7 */
	@Test
	public void testMetaFlushRunsOnlyWhileWritable() throws Exception {
		TestKeeperConfig config = watchConfig(true);
		config.setMetaFlushIntervalMilli(50);
		DefaultRedisKeeperServer server = (DefaultRedisKeeperServer) createRedisKeeperServer(config, true);
		server.initialize();
		DefaultReplicationStoreManager manager = (DefaultReplicationStoreManager) server.getReplicationStoreManager();
		manager.setMetaFlushCheckPeriodMilli(20);
		DefaultEndPoint master = new DefaultEndPoint("127.0.0.1", 0);
		try {
			server.start();
			server.setRedisKeeperServerState(new RedisKeeperServerStateActive(server, master));
			String dir = ((DefaultReplicationStore) server.getOrCreateReplicationStore()).getBaseDir().getName();
			assertPeriodicFlushRestoresManagerMeta(manager, dir);

			becomePrepare(server);
			Assert.assertTrue(manager.isReadOnly());
			Assert.assertTrue(manager.getLifecycleState().isStarted());
			Assert.assertNull(ReflectionTestUtils.getField(manager, "metaFlushFuture"));
			Object lastFlush = ReflectionTestUtils.getField(manager, "lastMetaFlushMilli");
			sleep(200);
			Assert.assertEquals("no tick while read-only", lastFlush,
					ReflectionTestUtils.getField(manager, "lastMetaFlushMilli"));

			server.getRedisKeeperServerState().becomeActive(master);
			Assert.assertFalse(manager.isReadOnly());
			Assert.assertNotNull(ReflectionTestUtils.getField(manager, "metaFlushFuture"));
			Assert.assertEquals(dir,
					((DefaultReplicationStore) server.getOrCreateReplicationStore()).getBaseDir().getName());
			assertPeriodicFlushRestoresManagerMeta(manager, dir);
		} finally {
			stopQuietly(server);
		}
	}

	private void assertPeriodicFlushRestoresManagerMeta(DefaultReplicationStoreManager manager, String dir)
			throws Exception {
		AsyncFile handle = (AsyncFile) ReflectionTestUtils.getField(manager, "managerMetaAsyncFile");
		Assert.assertNotNull(handle);
		Properties stale = new Properties();
		stale.setProperty("latest.store.dir", "stale");
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		stale.store(out, null);
		AsyncFileSystemHelper.writeAllBytes(asyncFileSystem(), handle, out.toByteArray(), "test write manager meta");
		waitConditionUntilTimeOut(() -> {
			try {
				long size = AsyncFileSystemHelper.await(() -> asyncFileSystem().size(handle), "size");
				Properties meta = new Properties();
				meta.load(new ByteArrayInputStream(
						AsyncFileSystemHelper.readAllBytes(asyncFileSystem(), handle, size, 0, "read")));
				return dir.equals(meta.getProperty("latest.store.dir"));
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}, 2000);
	}

	private static byte[] concat(byte[] a, byte[] b) {
		byte[] out = new byte[a.length + b.length];
		System.arraycopy(a, 0, out, 0, a.length);
		System.arraycopy(b, 0, out, a.length, b.length);
		return out;
	}

	private DefaultReplicationStoreManager spyWithDeferredRelease(DefaultRedisKeeperServer server) throws Exception {
		DefaultReplicationStoreManager manager =
				spy((DefaultReplicationStoreManager) server.getReplicationStoreManager());
		doAnswer(inv -> {
			inv.callRealMethod();
			return false;
		}).when(manager).releaseCurrentStore(false);
		server.setReplicationStoreManager(manager);
		return manager;
	}

	private DefaultRedisKeeperServer startActiveServer(TestKeeperConfig config, boolean tfsMode) throws Exception {
		DefaultRedisKeeperServer server = (DefaultRedisKeeperServer) createRedisKeeperServer(config, tfsMode);
		server.initialize();
		server.start();
		server.setRedisKeeperServerState(
				new RedisKeeperServerStateActive(server, new DefaultEndPoint("127.0.0.1", 0)));
		Assert.assertTrue(server.getOrCreateReplicationStore().checkOk());
		return server;
	}

	private TestKeeperConfig watchConfig(boolean enabled) {
		TestKeeperConfig config = new TestKeeperConfig();
		config.setPrepareStoreWatchEnabled(enabled);
		config.setPrepareWatchReopenIntervalMilli(50);
		config.setPrepareWatchCloseHoldMilli(10);
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
			server.getOrCreateReplicationStore();
			Assert.fail("PREPARE must refuse getOrCreateReplicationStore");
		} catch (RedisKeeperServerStateException expected) {
			Assert.assertTrue(expected.getMessage().contains("PREPARE"));
		}
		Assert.assertNull("PREPARE released the store, get must not reopen", server.getReplicationStore());
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

	private static long countPrepareCmdParserThreads() {
		return Thread.getAllStackTraces().keySet().stream()
				.filter(t -> t.getName() != null && t.getName().contains("prepare-cmd-parser"))
				.count();
	}

	@Override
	protected String getXpipeMetaConfigFile() {
		return "keeper-test.xml";
	}
}

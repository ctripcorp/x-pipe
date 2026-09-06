package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperContextTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.handler.CommandHandlerManager;
import com.ctrip.xpipe.redis.keeper.handler.keeper.KeeperCommandHandler;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.store.readonly.ReadOnlyCommandStore;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Phase KA: stage-1 cross-cutting acceptance. Phase-local ACs stay in their Phase tests.
 */
public class KeeperStageOneAcceptanceTest extends AbstractRedisKeeperContextTest {

	private static final String REPL_ID = "000000000000000000000000000000000000000A";

	private static final byte[] PING_CMD = "*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII);

	private final KeeperCommandHandler keeperCommandHandler = new KeeperCommandHandler();

	@Test
	public void testAc1_defaultSwitchesKeepPrepareGateClosed() throws Exception {
		KeeperConfig defaults = new DefaultKeeperConfig();
		Assert.assertFalse(defaults.isPrepareStoreWatchEnabled());
		Assert.assertFalse(defaults.isPubsubParseEnabled());
		TestKeeperConfig testDefaults = new TestKeeperConfig();
		Assert.assertFalse(testDefaults.isPrepareStoreWatchEnabled());
		Assert.assertFalse(testDefaults.isPubsubParseEnabled());

		DefaultRedisKeeperServer server = startActiveServer(new TestKeeperConfig(), true);
		try {
			server.getRedisKeeperServerState().becomePrepare(new DefaultEndPoint("127.0.0.1", randomPort()));
			Assert.assertEquals(KeeperState.PREPARE, server.getRedisKeeperServerState().keeperState());
			Assert.assertFalse(server.getReplicationStoreManager().isReadOnly());
			Assert.assertTrue(server.getReplicationStoreManager().getLifecycleState().isPositivelyStopped());
			Assert.assertNull(server.getPrepareWatcher());
			Assert.assertNull(server.getPrepareCmdParser());
			Assert.assertEquals(0, countThreads("prepare-watch"));
			Assert.assertEquals(0, countThreads("prepare-cmd-parser"));
			try {
				server.getReplicationStore();
				Assert.fail("PREPARE with defaults must refuse getReplicationStore");
			} catch (RedisKeeperServerStateException expected) {
				Assert.assertTrue(expected.getMessage().contains("PREPARE"));
			}
		} finally {
			stopQuietly(server);
		}
	}

	@Test
	public void testAc2d_productionCommandStoreSourcesUnchanged() throws Exception {
		String abstractStore = assertNoM5ReadOnlyBranch(new File(
				"src/main/java/com/ctrip/xpipe/redis/keeper/store/AbstractCommandStore.java"));
		Assert.assertTrue(abstractStore.contains("cmdWriter"));
		String defaultStore = assertNoM5ReadOnlyBranch(new File(
				"src/main/java/com/ctrip/xpipe/redis/keeper/store/DefaultCommandStore.java"));
		Assert.assertTrue(defaultStore.contains("extends AbstractCommandStore"));
	}

	@Test
	public void testAc3_occupyingWriterUnaffectedByConcurrentReadOnlyOpen() throws Exception {
		AsyncFileSystem fs = spy(createTestAsyncFileSystem());
		File base = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultReplicationStoreManager occupying = newManager(fs, base, runid);
		DefaultReplicationStoreManager watching = newManager(fs, base, runid);
		try {
			LifecycleHelper.initializeIfPossible(occupying);
			LifecycleHelper.startIfPossible(occupying);
			DefaultReplicationStore writable = (DefaultReplicationStore) occupying.create();
			seedCommands(writable);
			long writerBefore = writable.getCommandStore().totalLength();
			Assert.assertTrue(writerBefore > 0);

			LifecycleHelper.initializeIfPossible(watching);
			watching.setReadOnly(true);
			LifecycleHelper.startIfPossible(watching);
			clearInvocations(fs);

			DefaultReplicationStore readOnly = (DefaultReplicationStore) watching.getCurrent();
			Assert.assertTrue(readOnly.getCommandStore() instanceof ReadOnlyCommandStore);
			verify(fs, atLeastOnce()).open(anyString(), anyString(), anyList(), eq(false), anyString());
			verify(fs, never()).open(anyString(), anyString(), anyList(), eq(true), anyString());

			writable.appendCommands(Unpooled.wrappedBuffer(PING_CMD));
			writable.getCommandStore().rotateFileIfNecessary();
			Assert.assertTrue(writable.gc());
			Assert.assertTrue(writable.getCommandStore().totalLength() > writerBefore);
			Assert.assertSame(writable, occupying.getOpenedStore());
		} finally {
			stopDispose(watching);
			stopDispose(occupying);
			fs.shutdown();
		}
	}

	@Test
	public void testAc6b_readOnlyTotalLengthNeverExceedsWriter() throws Exception {
		AsyncFileSystem fs = createTestAsyncFileSystem();
		File base = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultReplicationStoreManager occupying = newManager(fs, base, runid);
		DefaultReplicationStoreManager watching = newManager(fs, base, runid);
		try {
			LifecycleHelper.initializeIfPossible(occupying);
			LifecycleHelper.startIfPossible(occupying);
			DefaultReplicationStore writable = (DefaultReplicationStore) occupying.create();
			seedCommands(writable);

			LifecycleHelper.initializeIfPossible(watching);
			watching.setReadOnly(true);
			LifecycleHelper.startIfPossible(watching);
			ReadOnlyCommandStore readOnly = (ReadOnlyCommandStore)
					((DefaultReplicationStore) watching.getCurrent()).getCommandStore();

			assertConservativeEnd(readOnly, writable.getCommandStore());
			long firstSeen = readOnly.totalLength();

			writable.appendCommands(Unpooled.wrappedBuffer(PING_CMD));
			long writerAfter = writable.getCommandStore().totalLength();
			Assert.assertTrue(writerAfter > firstSeen);
			sleep(ReadOnlyCommandStore.REOPEN_DEBOUNCE_MILLI + 20);
			readOnly.observeCurrentEnd();
			assertConservativeEnd(readOnly, writable.getCommandStore());
			Assert.assertTrue(readOnly.totalLength() >= firstSeen);
		} finally {
			stopDispose(watching);
			stopDispose(occupying);
			fs.shutdown();
		}
	}

	@Test
	public void testAc7_cmdTailBranchDoesNotLocateTailOfCmd() throws Exception {
		File source = new File(
				"src/main/java/com/ctrip/xpipe/redis/keeper/handler/keeper/GapAllowSyncHandler.java");
		Assert.assertTrue(source.isFile());
		String text = stripComments(new String(Files.readAllBytes(source.toPath()), StandardCharsets.UTF_8));
		int start = text.indexOf("request.offset == KEEPER_CMD_TAIL_SYNC_OFFSET");
		int end = text.indexOf("request.offset == -3", start);
		Assert.assertTrue(start >= 0 && end > start);
		String block = text.substring(start, end);
		Assert.assertFalse(block.contains("locateTailOfCmd"));
		Assert.assertTrue(block.contains("getEndOffset"));
		Assert.assertTrue(block.contains("markKeeperPartial"));
	}

	@Test
	public void testAc9_commandHandlerManagerRegistersPubSubAndConfig() {
		Set<String> commands = new HashSet<>(Arrays.asList(new CommandHandlerManager().getCommands()));
		Assert.assertTrue(commands.contains("subscribe"));
		Assert.assertTrue(commands.contains("psubscribe"));
		Assert.assertTrue(commands.contains("unsubscribe"));
		Assert.assertTrue(commands.contains("publish"));
		Assert.assertTrue(commands.contains("config"));
	}

	@Test
	public void testAc21_managerStartFailureDoesNotChangeSetStatePrepareReply() throws Exception {
		TestKeeperConfig config = new TestKeeperConfig();
		config.setPrepareStoreWatchEnabled(true);
		DefaultRedisKeeperServer server = startActiveServer(config, true);
		try {
			DefaultReplicationStoreManager manager =
					spy((DefaultReplicationStoreManager) server.getReplicationStoreManager());
			doThrow(new RuntimeException("inject-start")).when(manager).start();
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

	private DefaultReplicationStoreManager newManager(AsyncFileSystem fs, File base, String runid) {
		TestKeeperConfig config = new TestKeeperConfig();
		config.setReplicationStoreGcIntervalSeconds(60);
		config.setMinTimeMilliToGcAfterCreate(60_000);
		return new DefaultReplicationStoreManager(
				config, getReplId(), runid, base,
				createkeeperMonitor(), mock(SyncRateManager.class), createRedisOpParser(), null, fs);
	}

	private void seedCommands(DefaultReplicationStore store) throws Exception {
		store.psyncContinueFrom(REPL_ID, 1);
		store.appendCommands(Unpooled.wrappedBuffer(PING_CMD));
	}

	private static void assertConservativeEnd(ReadOnlyCommandStore readOnly, CommandStore writer) {
		long observed = readOnly.totalLength();
		long real = writer.totalLength();
		Assert.assertTrue("read-only totalLength=" + observed + " writer=" + real, observed <= real);
	}

	private static String assertNoM5ReadOnlyBranch(File source) throws Exception {
		Assert.assertTrue("missing " + source.getPath(), source.isFile());
		String text = stripComments(new String(Files.readAllBytes(source.toPath()), StandardCharsets.UTF_8));
		for (String forbidden : Arrays.asList("ReadOnlyCommandStore", "observeCurrentEnd", "setReadOnly",
				"isReadOnly", "prepareWatch", "MISS_BACKOFF", "REOPEN_DEBOUNCE")) {
			Assert.assertFalse(source.getName() + " must not contain " + forbidden, text.contains(forbidden));
		}
		return text;
	}

	private String invokeKeeperCommand(RedisKeeperServer server, String... args) throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel();
		DefaultRedisClient client = new DefaultRedisClient(channel, server);
		keeperCommandHandler.handle(args, client);
		Object outbound = channel.readOutbound();
		Assert.assertNotNull(outbound);
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

	private static void stopDispose(DefaultReplicationStoreManager manager) {
		try {
			LifecycleHelper.stopIfPossible(manager);
		} catch (Throwable ignore) {
		}
		try {
			LifecycleHelper.disposeIfPossible(manager);
		} catch (Throwable ignore) {
		}
	}

	private static long countThreads(String namePart) {
		return Thread.getAllStackTraces().keySet().stream()
				.filter(t -> t.getName() != null && t.getName().contains(namePart))
				.count();
	}

	private static String stripComments(String source) {
		String noBlock = source.replaceAll("/\\*[\\s\\S]*?\\*/", "");
		StringBuilder out = new StringBuilder(noBlock.length());
		for (String line : noBlock.split("\n", -1)) {
			int idx = line.indexOf("//");
			out.append(idx >= 0 ? line.substring(0, idx) : line).append('\n');
		}
		return out.toString();
	}

	@Override
	protected String getXpipeMetaConfigFile() {
		return "keeper-test.xml";
	}
}

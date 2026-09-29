package com.ctrip.xpipe.redis.keeper.prepare;

import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.Psync;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperContextTest;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServerState;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.handler.keeper.GapAllowSyncHandler;
import com.ctrip.xpipe.redis.keeper.handler.keeper.InfoHandler;
import com.ctrip.xpipe.redis.keeper.handler.keeper.RoleCommandHandler;
import com.ctrip.xpipe.redis.keeper.impl.DefaultKeeperRepl;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisClient;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateActive;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.store.meta.AbstractMetaStore;
import com.ctrip.xpipe.redis.keeper.store.readonly.ReadOnlyCommandStore;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.Invocation;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Phase HA：跨 Phase 验收（T-HA.1～4）。
 * T-HA.1 宿主两相在本包，才能驱动 {@code pollOnce} / {@code setClock}。
 */
public class KeeperHandleReworkAcceptanceTest extends AbstractRedisKeeperContextTest {

	private static final String REPL_ID = "000000000000000000000000000000000000000A";

	private static final int REOPEN_INTERVAL_MILLI = 50;

	private static final int CLOSE_HOLD_MILLI = 10;

	private TestKeeperConfig watchKeeperConfig;

	private final AtomicLong now = new AtomicLong();

	private final InfoHandler infoHandler = new InfoHandler();

	private final RoleCommandHandler roleHandler = new RoleCommandHandler();

	private final CmdTailProbe cmdTailProbe = new CmdTailProbe();

	@Before
	public void beforeKeeperHandleReworkAcceptanceTest() {
		watchKeeperConfig = new TestKeeperConfig();
		watchKeeperConfig.setReplicationStoreGcIntervalSeconds(60);
		watchKeeperConfig.setMinTimeMilliToGcAfterCreate(60_000);
		watchKeeperConfig.setPrepareWatchReopenIntervalMilli(REOPEN_INTERVAL_MILLI);
		watchKeeperConfig.setPrepareWatchCloseHoldMilli(CLOSE_HOLD_MILLI);
		now.set(0);
	}

	/**
	 * T-HA.1 / AC-5 / AC-5b：完整 PREPARE 生命周期后，两相都可用 INFO / ROLE / {@code ? -4}。
	 * 时钟推进相位，不睡 {@code WATCH_TICK_MILLI}。定位二分的 miss 分支在 OS；这里锁连续链
	 * {@code OPENED} 刷新的快照在关相仍保留（与 {@code LOCATE_MISS} 同一条内存路径）。
	 */
	@Test
	public void testTwoPhaseInfoRoleAndCmdTailAvailableInBothPhases() throws Exception {
		TestKeeperConfig config = new TestKeeperConfig();
		config.setPrepareStoreWatchEnabled(true);
		config.setPrepareWatchReopenIntervalMilli(REOPEN_INTERVAL_MILLI);
		config.setPrepareWatchCloseHoldMilli(CLOSE_HOLD_MILLI);
		DefaultRedisKeeperServer server = startActiveServer(config);
		try {
			DefaultReplicationStore writable = (DefaultReplicationStore) server.getReplicationStore();
			writable.psyncContinueFrom(REPL_ID, 1);
			writable.appendCommands(Unpooled.wrappedBuffer("*1\r\n$4\r\nPING\r\n".getBytes()));
			server.getRedisKeeperServerState().becomePrepare(new DefaultEndPoint("127.0.0.1", randomPort()));
			PrepareStoreWatcher watcher = server.getPrepareWatcher();
			Assert.assertNotNull(watcher);
			watcher.stop();
			waitConditionUntilTimeOut(() -> countPrepareWatchThreads() == 0);
			now.set(0);
			watcher.setClock(now::get);

			Assert.assertNotNull(server.getReplicationStore());
			watcher.pollOnce();
			Assert.assertEquals(PrepareStoreWatcher.Phase.OPEN, watcher.getPhase());

			now.addAndGet(REOPEN_INTERVAL_MILLI);
			watcher.pollOnce();
			Assert.assertEquals(PrepareStoreWatcher.Phase.CLOSED, watcher.getPhase());
			now.addAndGet(CLOSE_HOLD_MILLI);
			watcher.pollOnce();
			Assert.assertEquals(PrepareStoreWatcher.Phase.OPEN, watcher.getPhase());
			Assert.assertNotNull(watcher.getSnapshot());
			Assert.assertTrue(watcher.getSnapshot().getTotalLength() > 0);
			assertHostObservationCommands(server, watcher);

			now.addAndGet(REOPEN_INTERVAL_MILLI);
			watcher.pollOnce();
			Assert.assertEquals(PrepareStoreWatcher.Phase.CLOSED, watcher.getPhase());
			Assert.assertNotNull("closed phase must keep last OPENED snapshot (D49 locate-miss path)",
					watcher.getSnapshot());
			assertHostObservationCommands(server, watcher);
		} finally {
			stopQuietly(server);
		}
	}

	/**
	 * AC-5b ③：关相 MetaStore / {@code getCurrent} / 三个 offset getter / INFO / ROLE / {@code ? -4}
	 * 零 FS，且 {@code currentMeta} 未被清空（请求侧 {@code getCurrent()} 不懒开 manager-meta）。
	 */
	@Test
	public void testClosedPhaseObservationHasZeroFsCallsAndNoLazyOpen() throws Exception {
		AsyncFileSystem fs = spy(createTestAsyncFileSystem());
		File base = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultReplicationStoreManager occupying = newManager(fs, base, runid);
		DefaultReplicationStoreManager watching = newManager(fs, base, runid);
		PrepareStoreWatcher watcher = newWatcher(watching);
		try {
			seedOccupied(occupying);
			DefaultReplicationStore opened = openReadOnly(watching);
			watcher.pollOnce();
			now.addAndGet(REOPEN_INTERVAL_MILLI);
			watcher.pollOnce();
			now.addAndGet(CLOSE_HOLD_MILLI);
			watcher.pollOnce();
			Assert.assertEquals(PrepareStoreWatcher.Phase.OPEN, watcher.getPhase());
			Assert.assertNotNull(watcher.getSnapshot());
			now.addAndGet(REOPEN_INTERVAL_MILLI);
			watcher.pollOnce();
			Assert.assertEquals(PrepareStoreWatcher.Phase.CLOSED, watcher.getPhase());
			ReadOnlyCommandStore cmd = cmdStore(opened);
			Assert.assertFalse(cmd.isHandleOpen());
			Assert.assertFalse(metaStore(opened).isMetaHandleOpen());
			Assert.assertFalse(watching.isManagerMetaHandleOpen());
			clearInvocations(fs);

			Assert.assertSame(opened, watching.getCurrent());
			Assert.assertNotNull("currentMeta must stay cached in closed phase", watching.getCurrentMetaCache());
			Assert.assertEquals(REPL_ID, opened.getMetaStore().getCurReplStageReplId());
			Assert.assertNotNull(opened.getMetaStore().getCurrentReplStage());
			Assert.assertEquals(1L, opened.getMetaStore().getCurrentReplStage().getBegOffsetRepl());
			Assert.assertTrue(cmd.totalLength() > 0);
			Assert.assertTrue(cmd.lowestAvailableOffset() >= 0);
			Assert.assertTrue(cmd.startOffsetOf(Math.max(0L, cmd.totalLength() - 1)) >= 0);

			RedisKeeperServer server = mockPrepareServer(opened, watcher.getSnapshot());
			String info = invokeInfo(server, "replication");
			Assert.assertTrue(info.contains("state:" + KeeperState.PREPARE));
			Assert.assertTrue(info.contains("master_repl_offset:" + watcher.getSnapshot().getReplOffset()));
			Object[] payload = new ArrayParser().read(Unpooled.wrappedBuffer(invokeRole(server).getBytes())).getPayload();
			SlaveRole role = new SlaveRole(payload);
			Assert.assertEquals(Server.SERVER_ROLE.KEEPER, role.getServerRole());
			Assert.assertEquals(MASTER_STATE.REDIS_REPL_NONE, role.getMasterState());
			Assert.assertEquals(watcher.getSnapshot().getReplOffset(), role.getMasterOffset());

			RedisSlave slave = mock(RedisSlave.class);
			when(slave.isOpen()).thenReturn(true);
			when(server.getKeeperRepl()).thenReturn(new DefaultKeeperRepl(opened));
			when(server.getKeeperConfig()).thenReturn(watchKeeperConfig);
			long expected = new DefaultKeeperRepl(opened).getEndOffset() + 1;
			Assert.assertEquals(expected, cmdTailProbe.probeOffset(server, slave));

			Collection<Invocation> leftover = mockingDetails(fs).getInvocations();
			Assert.assertTrue("closed-phase must not touch FS: " + leftover, leftover.isEmpty());
			Assert.assertFalse(watching.isManagerMetaHandleOpen());
		} finally {
			watcher.stop();
			stopDispose(watching);
			stopDispose(occupying);
			fs.shutdown();
		}
	}

	/**
	 * AC-6 ④ / AC-7b：Reader 永不开关句柄；退避不持 {@code handleLock}；退避不在 IO / 命令 / watch 线程。
	 */
	@Test
	public void testReaderBackoffOwnershipAndThreadGates() throws Exception {
		String reader = stripComments(readSource(
				"src/main/java/com/ctrip/xpipe/redis/keeper/store/readonly/ReopenOffsetCommandReader.java"));
		Assert.assertFalse(reader.contains("fs.open"));
		Assert.assertFalse(reader.contains("fs.close"));
		Assert.assertFalse(reader.contains("openAndObserve"));
		Assert.assertFalse(reader.contains("closeHandleForCycle"));

		File storeFile = new File("src/main/java/com/ctrip/xpipe/redis/keeper/store/readonly/ReadOnlyCommandStore.java");
		Assert.assertFalse(methodBody(storeFile, "void missAndBackoff()").contains("handleLock"));
		String listener = methodBody(storeFile, "public void addCommandsListener");
		Assert.assertTrue(listener.contains("missAndBackoff"));
		Assert.assertFalse(listener.contains("handleLock"));
		Assert.assertFalse(stripComments(new String(Files.readAllBytes(storeFile.toPath()), StandardCharsets.UTF_8))
				.contains("Thread.currentThread().getName()"));

		String watcher = stripComments(readSource(
				"src/main/java/com/ctrip/xpipe/redis/keeper/prepare/PrepareStoreWatcher.java"));
		Assert.assertFalse(watcher.contains("Thread.sleep"));
		Assert.assertFalse(watcher.contains("missAndBackoff"));
		Assert.assertFalse(stripComments(readSource(
				"src/main/java/com/ctrip/xpipe/redis/keeper/handler/keeper/InfoHandler.java"))
				.contains("Thread.sleep"));
		Assert.assertFalse(stripComments(readSource(
				"src/main/java/com/ctrip/xpipe/redis/keeper/handler/keeper/RoleCommandHandler.java"))
				.contains("Thread.sleep"));
		Assert.assertTrue(stripComments(readSource(
				"src/main/java/com/ctrip/xpipe/redis/keeper/handler/keeper/GapAllowSyncHandler.java"))
				.contains("processPsyncSequentially"));
		Assert.assertTrue(stripComments(readSource(
				"src/main/java/com/ctrip/xpipe/redis/keeper/prepare/PrepareCmdParser.java"))
				.contains("\"prepare-cmd-parser\""));
	}

	/**
	 * T-HA.4：进程内 TFS IT 不得写成「1s 后可见」。墙钟新鲜度不可证（共用 {@code fileEntries}）。
	 */
	@Test
	public void testTfsIntegratedTestsDoNotAssertFreshnessAfterOneSecond() throws Exception {
		File tfsDir = tfsIntegratedKeeperDir();
		Assert.assertTrue("missing TFS IT dir: " + tfsDir.getAbsolutePath(), tfsDir.isDirectory());
		File base = new File(tfsDir, "AbstractTfsKeeperIntegrated.java");
		Assert.assertTrue(base.isFile());
		String baseText = new String(Files.readAllBytes(base.toPath()), StandardCharsets.UTF_8);
		Assert.assertTrue(baseText.contains("T-HA.4"));
		Assert.assertTrue(baseText.contains("两相节奏不破坏既有闭环"));
		File[] sources = tfsDir.listFiles((dir, name) -> name.endsWith(".java"));
		Assert.assertNotNull(sources);
		for (File source : sources) {
			String text = new String(Files.readAllBytes(source.toPath()), StandardCharsets.UTF_8);
			Assert.assertFalse(source.getName() + " must not assert 1s-later visibility",
					text.contains("1s 后可见"));
			Assert.assertFalse(source.getName() + " must not assert visible-after-1s",
					text.contains("visible after 1s"));
		}
	}

	private RedisKeeperServer mockPrepareServer(DefaultReplicationStore opened, PrepareWatchSnapshot snapshot) {
		RedisKeeperServer server = mock(RedisKeeperServer.class);
		RedisKeeperServerState state = mock(RedisKeeperServerState.class);
		when(server.getRedisKeeperServerState()).thenReturn(state);
		when(state.keeperState()).thenReturn(KeeperState.PREPARE);
		when(state.getMaster()).thenReturn(new DefaultEndPoint("10.0.0.1", 6379));
		when(server.getRedisMaster()).thenReturn(null);
		when(server.role()).thenReturn(Server.SERVER_ROLE.KEEPER);
		when(server.getOpenedStore()).thenReturn(opened);
		when(server.getPrepareWatchSnapshot()).thenReturn(snapshot);
		when(server.slaves()).thenReturn(Collections.<RedisSlave>emptySet());
		return server;
	}

	private String invokeInfo(RedisKeeperServer server, String... args) throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel();
		RedisClient<?> client = new DefaultRedisClient(channel, server);
		infoHandler.handle(args, client);
		String raw = readOutbound(channel);
		int idx = raw.indexOf("\r\n");
		Assert.assertTrue(raw.startsWith("$"));
		return raw.substring(idx + 2, raw.length() - 2);
	}

	private String invokeRole(RedisKeeperServer server) throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel();
		RedisClient<?> client = new DefaultRedisClient(channel, server);
		roleHandler.handle(new String[0], client);
		return readOutbound(channel);
	}

	private static String readOutbound(EmbeddedChannel channel) {
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

	private void assertHostObservationCommands(DefaultRedisKeeperServer server, PrepareStoreWatcher watcher)
			throws Exception {
		PrepareWatchSnapshot snapshot = watcher.getSnapshot();
		Assert.assertNotNull(snapshot);
		String info = invokeInfo(server, "replication");
		Assert.assertTrue(info.contains("state:" + KeeperState.PREPARE));
		Assert.assertTrue(info.contains("master_replid:" + snapshot.getMasterReplId()));
		Assert.assertTrue(info.contains("master_repl_offset:" + snapshot.getReplOffset()));
		Object[] payload = new ArrayParser().read(Unpooled.wrappedBuffer(invokeRole(server).getBytes())).getPayload();
		SlaveRole role = new SlaveRole(payload);
		Assert.assertEquals(Server.SERVER_ROLE.KEEPER, role.getServerRole());
		Assert.assertEquals(MASTER_STATE.REDIS_REPL_NONE, role.getMasterState());
		Assert.assertEquals(snapshot.getReplOffset(), role.getMasterOffset());
		RedisSlave slave = mock(RedisSlave.class);
		when(slave.isOpen()).thenReturn(true);
		Assert.assertEquals(server.getKeeperRepl().getEndOffset() + 1, cmdTailProbe.probeOffset(server, slave));
	}

	private DefaultRedisKeeperServer startActiveServer(TestKeeperConfig config) throws Exception {
		DefaultRedisKeeperServer server = (DefaultRedisKeeperServer) createRedisKeeperServer(config, true);
		server.initialize();
		server.start();
		server.setRedisKeeperServerState(
				new RedisKeeperServerStateActive(server, new DefaultEndPoint("127.0.0.1", 0)));
		Assert.assertTrue(server.getReplicationStore().checkOk());
		return server;
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

	private PrepareStoreWatcher newWatcher(DefaultReplicationStoreManager manager) {
		PrepareStoreWatcher watcher = new PrepareStoreWatcher(manager, watchKeeperConfig,
				PrepareStoreChangeListener.NOOP);
		watcher.setClock(now::get);
		return watcher;
	}

	private void seedOccupied(DefaultReplicationStoreManager occupying) throws Exception {
		LifecycleHelper.initializeIfPossible(occupying);
		LifecycleHelper.startIfPossible(occupying);
		DefaultReplicationStore writable = (DefaultReplicationStore) occupying.create();
		writable.psyncContinueFrom(REPL_ID, 1);
		writable.appendCommands(Unpooled.wrappedBuffer("*1\r\n$4\r\nPING\r\n".getBytes()));
	}

	private DefaultReplicationStore openReadOnly(DefaultReplicationStoreManager watching) throws Exception {
		LifecycleHelper.initializeIfPossible(watching);
		watching.setReadOnly(true);
		LifecycleHelper.startIfPossible(watching);
		ReplicationStore store = watching.getCurrent();
		Assert.assertNotNull(store);
		return (DefaultReplicationStore) store;
	}

	private DefaultReplicationStoreManager newManager(AsyncFileSystem fs, File base, String runid) {
		return new DefaultReplicationStoreManager(
				watchKeeperConfig, getReplId(), runid, base,
				createkeeperMonitor(), mock(SyncRateManager.class), createRedisOpParser(), null, fs);
	}

	private static ReadOnlyCommandStore cmdStore(DefaultReplicationStore store) {
		CommandStore cmdStore = store.getCommandStore();
		Assert.assertTrue(cmdStore instanceof ReadOnlyCommandStore);
		return (ReadOnlyCommandStore) cmdStore;
	}

	private static AbstractMetaStore metaStore(DefaultReplicationStore store) {
		Assert.assertTrue(store.getMetaStore() instanceof AbstractMetaStore);
		return (AbstractMetaStore) store.getMetaStore();
	}

	private static File tfsIntegratedKeeperDir() {
		File moduleRelative = new File(
				"../redis-integration-test/src/test/java/com/ctrip/xpipe/redis/integratedtest/keeper");
		if (moduleRelative.isDirectory()) {
			return moduleRelative;
		}
		return new File("redis/redis-integration-test/src/test/java/com/ctrip/xpipe/redis/integratedtest/keeper");
	}

	private static String readSource(String path) throws Exception {
		File source = new File(path);
		Assert.assertTrue("missing " + path, source.isFile());
		return new String(Files.readAllBytes(source.toPath()), StandardCharsets.UTF_8);
	}

	private static String methodBody(File source, String signature) throws Exception {
		Assert.assertTrue("missing " + source.getPath(), source.isFile());
		String text = new String(Files.readAllBytes(source.toPath()), StandardCharsets.UTF_8);
		int start = text.indexOf(signature);
		Assert.assertTrue("signature not found: " + signature, start >= 0);
		int open = text.indexOf('{', start);
		Assert.assertTrue(open > start);
		int depth = 0;
		int end = -1;
		for (int i = open; i < text.length(); i++) {
			char c = text.charAt(i);
			if (c == '{') {
				depth++;
			} else if (c == '}') {
				depth--;
				if (depth == 0) {
					end = i;
					break;
				}
			}
		}
		Assert.assertTrue("unbalanced braces for " + signature, end > open);
		return text.substring(open, end);
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

	@Override
	protected String getXpipeMetaConfigFile() {
		return "keeper-test.xml";
	}

	private static final class CmdTailProbe extends GapAllowSyncHandler {
		@Override
		protected SyncRequest parseRequest(String[] args, RedisSlave redisSlave) {
			return null;
		}

		@Override
		public String[] getCommands() {
			return new String[0];
		}

		long probeOffset(RedisKeeperServer server, RedisSlave slave) throws Exception {
			SyncAction action = anaRequest(
					SyncRequest.psync("?", Psync.KEEPER_CMD_TAIL_SYNC_OFFSET), server, slave);
			Assert.assertNotNull(action);
			Assert.assertFalse(action.isFull());
			Assert.assertTrue(action.isKeeperPartial());
			return action.getReplOffset();
		}
	}
}

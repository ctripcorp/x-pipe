package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.core.server.FakeRedisServer;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.*;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.handler.keeper.InfoHandler;
import com.ctrip.xpipe.redis.keeper.handler.keeper.KeeperCommandHandler;
import com.ctrip.xpipe.redis.keeper.handler.keeper.RoleCommandHandler;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
import io.netty.buffer.Unpooled;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ctrip.xpipe.redis.keeper.SLAVE_STATE.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author wenchao.meng
 *
 *         2016年4月21日 下午5:42:29
 */
public class DefaultRedisKeeperServerTest extends AbstractRedisKeeperContextTest {

	private final KeeperCommandHandler keeperCommandHandler = new KeeperCommandHandler();

	@Before
	public void beforeDefaultRedisKeeperServerTest() throws Exception {
	}

	@Test
	public void testLongTask() throws Exception {

		RedisKeeperServer redisKeeperServer = createRedisKeeperServer();
		redisKeeperServer.initialize();
		redisKeeperServer.start();
		redisKeeperServer.processCommandSequentially(() -> sleep(1100));

	}

	@Test
	public void testStopGetReplicationStore() throws Exception {

		RedisKeeperServer redisKeeperServer = createRedisKeeperServer();

		try{
			redisKeeperServer.getReplicationStore();
			Assert.fail();
		}catch (Exception e){
			logger.info("error", e);
		}
		redisKeeperServer.initialize();
		redisKeeperServer.getReplicationStore();

		redisKeeperServer.start();
		redisKeeperServer.getReplicationStore();

		// Server.stop → Manager.stop: releaseCurrentStore + isPositivelyStopped refuse reopen
		redisKeeperServer.stop();
		try {
			redisKeeperServer.getReplicationStore();
			Assert.fail();
		} catch (Exception e) {
			logger.info("after stop", e);
		}

		redisKeeperServer.dispose();

		logger.info("after dispose");
		try{
			redisKeeperServer.getReplicationStore();
			Assert.fail();
		}catch (Exception e){
			logger.info("{}", e);
		}
	}

	/**
	 * T-R.10 / T-R.11⑦: ACTIVE↔BACKUP must not {@code Manager.stop()}/{@code releaseCurrentStore}
	 * (m1 §4.5: target BACKUP skips PREPARE Step1).
	 */
	@Test
	public void testActiveToBackupDoesNotReleaseStore() throws Exception {
		DefaultRedisKeeperServer redisKeeperServer = (DefaultRedisKeeperServer) createRedisKeeperServer();
		redisKeeperServer.initialize();
		redisKeeperServer.start();
		try {
			redisKeeperServer.setRedisKeeperServerState(
					new RedisKeeperServerStateActive(redisKeeperServer, new DefaultEndPoint("127.0.0.1", 0)));
			ReplicationStore storeBefore = redisKeeperServer.getReplicationStore();
			Assert.assertTrue(storeBefore.checkOk());

			ReplicationStoreManager manager = spy(redisKeeperServer.getReplicationStoreManager());
			redisKeeperServer.setReplicationStoreManager(manager);

			redisKeeperServer.getRedisKeeperServerState()
					.becomeBackup(new DefaultEndPoint("127.0.0.1", randomPort()));

			Assert.assertTrue(redisKeeperServer.getRedisKeeperServerState() instanceof RedisKeeperServerStateBackup);
			Assert.assertTrue(manager.getLifecycleState().isStarted());
			ReplicationStore storeAfter = redisKeeperServer.getReplicationStore();
			Assert.assertTrue(storeAfter.checkOk());
			Assert.assertSame(storeBefore, storeAfter);
			verify(manager, never()).stop();
			verify(manager, never()).releaseCurrentStore();
		} finally {
			redisKeeperServer.stop();
			redisKeeperServer.dispose();
		}
	}

	/**
	 * T-R.11①: {@code SETSTATE PREPARE} → +OK; {@code GETSTATE} → PREPARE.
	 */
	@Test
	public void testKeeperCommandSetStatePrepareOkAndGetState() throws Exception {
		DefaultRedisKeeperServer redisKeeperServer = (DefaultRedisKeeperServer) createRedisKeeperServer();
		redisKeeperServer.initialize();
		redisKeeperServer.start();
		try {
			redisKeeperServer.setRedisKeeperServerState(
					new RedisKeeperServerStateActive(redisKeeperServer, new DefaultEndPoint("127.0.0.1", 0)));
			Assert.assertTrue(redisKeeperServer.getReplicationStore().checkOk());

			String setResp = invokeKeeperCommand(redisKeeperServer, "setstate", "PREPARE", "127.0.0.1", String.valueOf(randomPort()));
			Assert.assertEquals("+" + RedisProtocol.OK + "\r\n", setResp);
			Assert.assertEquals(KeeperState.PREPARE, redisKeeperServer.getRedisKeeperServerState().keeperState());

			String getResp = invokeKeeperCommand(redisKeeperServer, "getstate");
			Assert.assertEquals("+" + KeeperState.PREPARE + "\r\n", getResp);
		} finally {
			redisKeeperServer.stop();
			redisKeeperServer.dispose();
		}
	}

	/**
	 * T-R.11②: ACTIVE→PREPARE clears master/slaves, drops currentStore, keeps latest.store.dir (close≠destroy).
	 */
	@Test
	public void testActiveToPrepareReleasesLease() throws Exception {
		FakeRedisServer fakeMaster = startFakeRedisServer();
		DefaultRedisKeeperServer redisKeeperServer = (DefaultRedisKeeperServer) createRedisKeeperServer();
		redisKeeperServer.initialize();
		redisKeeperServer.start();
		try {
			Endpoint master = localHostEndpoint(fakeMaster.getPort());
			redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateActive(redisKeeperServer, master));
			redisKeeperServer.reconnectMaster();
			waitConditionUntilTimeOut(() -> fakeMaster.getConnected() == 1);
			Assert.assertNotNull(redisKeeperServer.getRedisMaster());

			ReplicationStore storeBefore = redisKeeperServer.getReplicationStore();
			File storeDir = new File(storeBefore.toString().substring("ReplicationStore:".length()));
			Assert.assertTrue(storeDir.isDirectory());

			EmbeddedChannel slaveChannel = new EmbeddedChannel();
			slaveChannel.closeFuture().addListener(f -> redisKeeperServer.clientDisconnected(slaveChannel));
			RedisSlave slave = redisKeeperServer.clientConnected(slaveChannel).becomeSlave();
			slave.markPsyncProcessed();
			Assert.assertEquals(1, redisKeeperServer.slaves().size());

			DefaultReplicationStoreManager manager =
					(DefaultReplicationStoreManager) redisKeeperServer.getReplicationStoreManager();

			redisKeeperServer.getRedisKeeperServerState().becomePrepare(new DefaultEndPoint("127.0.0.1", randomPort()));

			Assert.assertEquals(KeeperState.PREPARE, redisKeeperServer.getRedisKeeperServerState().keeperState());
			Assert.assertNull(redisKeeperServer.getRedisMaster());
			waitConditionUntilTimeOut(() -> redisKeeperServer.slaves().isEmpty());
			Assert.assertFalse(((DefaultRedisSlave) slave).isOpen());
			Assert.assertNull(manager.getCurrent());
			Assert.assertTrue(manager.getLifecycleState().isPositivelyStopped());
			Assert.assertTrue("PREPARE must not destroy latest.store.dir", storeDir.isDirectory());
			try {
				redisKeeperServer.getReplicationStore();
				Assert.fail("PREPARE must refuse getReplicationStore");
			} catch (Exception expected) {
				logger.info("prepare gate ok", expected);
			}
		} finally {
			redisKeeperServer.stop();
			redisKeeperServer.dispose();
		}
	}

	/**
	 * D34 / T-17.4: after SETSTATE PREPARE, ROLE / INFO REPLICATION / INFO ALL succeed with state=PREPARE;
	 * getReplicationStore gate remains closed.
	 */
	@Test
	public void testPrepareObservationCommandsCompatible() throws Exception {
		DefaultRedisKeeperServer redisKeeperServer = (DefaultRedisKeeperServer) createRedisKeeperServer();
		redisKeeperServer.initialize();
		redisKeeperServer.start();
		try {
			Endpoint master = new DefaultEndPoint("127.0.0.1", 0);
			redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateActive(redisKeeperServer, master));
			Assert.assertTrue(redisKeeperServer.getReplicationStore().checkOk());

			String setResp = invokeKeeperCommand(redisKeeperServer, "setstate", "PREPARE", "10.0.0.1", "6380");
			Assert.assertEquals("+" + RedisProtocol.OK + "\r\n", setResp);
			Assert.assertEquals(KeeperState.PREPARE, redisKeeperServer.getRedisKeeperServerState().keeperState());

			try {
				redisKeeperServer.getReplicationStore();
				Assert.fail("PREPARE must refuse getReplicationStore");
			} catch (Exception expected) {
				logger.info("prepare gate ok", expected);
			}

			EmbeddedChannel roleChannel = new EmbeddedChannel();
			new RoleCommandHandler().handle(new String[0], new DefaultRedisClient(roleChannel, redisKeeperServer));
			ByteBuf roleBuf = roleChannel.readOutbound();
			Assert.assertNotNull(roleBuf);
			String roleRaw = ByteBufUtils.readToString(roleBuf.duplicate());
			roleBuf.release();
			Assert.assertFalse(roleRaw.startsWith("-"));
			SlaveRole slaveRole = new SlaveRole(new ArrayParser().read(Unpooled.wrappedBuffer(roleRaw.getBytes())).getPayload());
			Assert.assertEquals(MASTER_STATE.REDIS_REPL_NONE, slaveRole.getMasterState());
			Assert.assertEquals(-1L, slaveRole.getMasterOffset());

			String replicationInfo = invokeInfoCommand(redisKeeperServer, "replication");
			Assert.assertTrue(replicationInfo.contains("state:" + KeeperState.PREPARE));
			Assert.assertEquals(KeeperState.PREPARE.name(), new InfoResultExtractor(replicationInfo).getKeeperState());

			String allInfo = invokeInfoCommand(redisKeeperServer, "all");
			Assert.assertTrue(allInfo.contains("state:" + KeeperState.PREPARE));
			Assert.assertFalse(allInfo.startsWith("-"));
		} finally {
			redisKeeperServer.stop();
			redisKeeperServer.dispose();
		}
	}

	/**
	 * T-R.11③: PREPARE is idempotent — second SETSTATE PREPARE still OK.
	 */
	@Test
	public void testPrepareIdempotent() throws Exception {
		DefaultRedisKeeperServer redisKeeperServer = (DefaultRedisKeeperServer) createRedisKeeperServer();
		redisKeeperServer.initialize();
		redisKeeperServer.start();
		try {
			Endpoint master = new DefaultEndPoint("127.0.0.1", 0);
			redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateActive(redisKeeperServer, master));
			redisKeeperServer.getRedisKeeperServerState().becomePrepare(master);
			Assert.assertEquals(KeeperState.PREPARE, redisKeeperServer.getRedisKeeperServerState().keeperState());

			String resp = invokeKeeperCommand(redisKeeperServer, "setstate", "PREPARE", "10.0.0.1", "6380");
			Assert.assertEquals("+" + RedisProtocol.OK + "\r\n", resp);
			Assert.assertEquals(KeeperState.PREPARE, redisKeeperServer.getRedisKeeperServerState().keeperState());
			Assert.assertTrue(redisKeeperServer.getReplicationStoreManager().getLifecycleState().isPositivelyStopped());
		} finally {
			redisKeeperServer.stop();
			redisKeeperServer.dispose();
		}
	}

	/**
	 * T-R.9 / T-R.11④: PREPARE → ACTIVE reopens the same {@code latest.store.dir}; meta remains readable.
	 */
	@Test
	public void testPrepareToActiveReopensLatestStoreDir() throws Exception {
		DefaultRedisKeeperServer redisKeeperServer = (DefaultRedisKeeperServer) createRedisKeeperServer();
		redisKeeperServer.initialize();
		redisKeeperServer.start();
		try {
			Endpoint master = new DefaultEndPoint("127.0.0.1", 0);
			redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateActive(redisKeeperServer, master));
			ReplicationStore storeBefore = redisKeeperServer.getReplicationStore();
			storeBefore.getMetaStore().becomeActive();
			String latestStoreId = storeBefore.toString();
			String replIdBefore = storeBefore.getMetaStore().getReplId();
			Assert.assertTrue(storeBefore.checkOk());

			redisKeeperServer.getRedisKeeperServerState().becomePrepare(master);
			Assert.assertEquals(KeeperState.PREPARE, redisKeeperServer.getRedisKeeperServerState().keeperState());
			Assert.assertTrue(redisKeeperServer.getReplicationStoreManager().getLifecycleState().isPositivelyStopped());
			try {
				redisKeeperServer.getReplicationStore();
				Assert.fail("PREPARE must refuse getReplicationStore");
			} catch (Exception expected) {
				logger.info("prepare gate ok", expected);
			}

			redisKeeperServer.getRedisKeeperServerState()
					.becomeActive(new DefaultEndPoint("127.0.0.1", randomPort()));
			Assert.assertEquals(KeeperState.ACTIVE, redisKeeperServer.getRedisKeeperServerState().keeperState());
			Assert.assertTrue(redisKeeperServer.getReplicationStoreManager().getLifecycleState().isStarted());
			ReplicationStore storeAfter = redisKeeperServer.getReplicationStore();
			Assert.assertTrue(storeAfter.checkOk());
			Assert.assertEquals(latestStoreId, storeAfter.toString());
			Assert.assertEquals(replIdBefore, storeAfter.getMetaStore().getReplId());
			Assert.assertEquals(KeeperState.ACTIVE, storeAfter.getMetaStore().dupReplicationStoreMeta().getKeeperState());
		} finally {
			redisKeeperServer.stop();
			redisKeeperServer.dispose();
		}
	}

	/**
	 * T-R.11⑤ (server gate): after PREPARE, hot path refuses reopen; latest dir remains on disk.
	 * Manager {@code gc()} no-op while stopped is covered in DefaultReplicationStoreManagerTest.
	 */
	@Test
	public void testPrepareRefusesReopenAndKeepsLatestDir() throws Exception {
		DefaultRedisKeeperServer redisKeeperServer = (DefaultRedisKeeperServer) createRedisKeeperServer();
		redisKeeperServer.initialize();
		redisKeeperServer.start();
		try {
			Endpoint master = new DefaultEndPoint("127.0.0.1", 0);
			redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateActive(redisKeeperServer, master));
			ReplicationStore store = redisKeeperServer.getReplicationStore();
			File storeDir = new File(store.toString().substring("ReplicationStore:".length()));
			Assert.assertTrue(storeDir.isDirectory());

			redisKeeperServer.getRedisKeeperServerState().becomePrepare(master);
			DefaultReplicationStoreManager manager =
					(DefaultReplicationStoreManager) redisKeeperServer.getReplicationStoreManager();
			Assert.assertTrue(manager.getLifecycleState().isPositivelyStopped());
			Assert.assertNull(manager.getCurrent());
			Assert.assertTrue(storeDir.isDirectory());
			try {
				manager.createIfNotExist();
				Assert.fail("createIfNotExist must refuse after PREPARE stop");
			} catch (IOException expected) {
				logger.info("stopped create gate ok", expected);
			}
			Assert.assertTrue(storeDir.isDirectory());
		} finally {
			redisKeeperServer.stop();
			redisKeeperServer.dispose();
		}
	}

	/**
	 * T-R.11⑥: lease release failure → Handler Redis ERROR (not swallowed).
	 */
	@Test
	public void testPrepareFailureReturnsRedisError() throws Exception {
		DefaultRedisKeeperServer redisKeeperServer = (DefaultRedisKeeperServer) createRedisKeeperServer();
		redisKeeperServer.initialize();
		redisKeeperServer.start();
		ReplicationStoreManager manager = null;
		try {
			redisKeeperServer.setRedisKeeperServerState(
					new RedisKeeperServerStateActive(redisKeeperServer, new DefaultEndPoint("127.0.0.1", 0)));
			Assert.assertTrue(redisKeeperServer.getReplicationStore().checkOk());

			manager = spy(redisKeeperServer.getReplicationStoreManager());
			doThrow(new IOException("inject-prepare-stop-fail")).when(manager).stop();
			redisKeeperServer.setReplicationStoreManager(manager);

			String resp = invokeKeeperCommand(redisKeeperServer, "setstate", "PREPARE", "127.0.0.1", "6379");
			Assert.assertTrue("expect Redis ERROR, got: " + resp, resp.startsWith("-"));
			Assert.assertTrue(resp.contains("inject-prepare-stop-fail") || resp.contains("lease release failed")
					|| resp.contains("stop replicationStoreManager failed"));
		} finally {
			if (manager != null) {
				doCallRealMethod().when(manager).stop();
			}
			redisKeeperServer.stop();
			redisKeeperServer.dispose();
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

	private String invokeInfoCommand(RedisKeeperServer server, String section) throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel();
		new InfoHandler().handle(new String[]{section}, new DefaultRedisClient(channel, server));
		Object outbound = channel.readOutbound();
		Assert.assertNotNull(outbound);
		Assert.assertTrue(outbound instanceof ByteBuf);
		ByteBuf buf = (ByteBuf) outbound;
		try {
			String raw = ByteBufUtils.readToString(buf.duplicate());
			Assert.assertFalse("INFO must not return Redis ERROR: " + raw, raw.startsWith("-"));
			Assert.assertTrue(raw.startsWith("$"));
			int idx = raw.indexOf("\r\n");
			return raw.substring(idx + 2, raw.length() - 2);
		} finally {
			buf.release();
		}
	}

	@Test
	public void testSetState() throws Exception {

		RedisKeeperServer redisKeeperServer = createRedisKeeperServer();
		for (int i = 0; i < 10; i++) {

			RedisKeeperServerState redisKeeperServerState = Mockito.mock(RedisKeeperServerState.class);
			long begin = System.currentTimeMillis();
			redisKeeperServer.setRedisKeeperServerState(redisKeeperServerState);
			long end = System.currentTimeMillis();
			if (end - begin > 200) {
				logger.info("[testSetState]i:{}, {}", i, end - begin);
				Assert.fail();
			}
		}

	}

	@Test
	public void testCompareAndDo() throws Exception {

		RedisKeeperServer redisKeeperServer = createRedisKeeperServer();
		RedisClient redisClient = Mockito.mock(RedisClient.class);

		RedisKeeperServerStateBackup backup = new RedisKeeperServerStateBackup(redisKeeperServer);
		redisKeeperServer.setRedisKeeperServerState(backup);

		assertFalse(backup.psync(redisClient, new String[] {}));
		;

		redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateActive(redisKeeperServer));

		Assert.assertTrue(backup.psync(redisClient, new String[] {}));
	}


	@Test
	public void testConcurrentSetRdbDumper() throws Exception {


		int concurrentCount = 5;
		RdbDumper dump1 = mock(RdbDumper.class);

		Assert.assertTrue(new SetRdbDumperException(dump1).isCancelSlave());

		RedisKeeperServer redisKeeperServer = createRedisKeeperServer();
		CountDownLatch latch = new CountDownLatch(concurrentCount);
		CyclicBarrier barrier = new CyclicBarrier(concurrentCount);

		AtomicBoolean success = new AtomicBoolean(true);

		for(int i=0;i<concurrentCount;i++){

			executors.execute(() -> {
				try {
					barrier.await();
					redisKeeperServer.setRdbDumper(dump1);
				} catch (SetRdbDumperException e) {
					success.set(false);
				} catch (Exception e) {
					logger.error("[run]", e);
				} finally {
					latch.countDown();
				}
			});
		}

		latch.await();
		assertFalse(success.get());
	}



	@Test
	public void testRdbDumperTooQuick() throws Exception {

		int rdbDumpMinIntervalMilli = 100;
		TestKeeperConfig keeperConfig = new TestKeeperConfig();
		keeperConfig.setRdbDumpMinIntervalMilli(rdbDumpMinIntervalMilli);
		RedisKeeperServer redisKeeperServer = createRedisKeeperServer(keeperConfig);

		RdbDumper dump1 = mock(RdbDumper.class);

		redisKeeperServer.setRdbDumper(dump1);

		redisKeeperServer.clearRdbDumper(dump1, false);

		// too quick
		// force can success
		redisKeeperServer.setRdbDumper(dump1, true);
		redisKeeperServer.clearRdbDumper(dump1, false);

		try {
			redisKeeperServer.setRdbDumper(dump1);
			Assert.fail();
		} catch (SetRdbDumperException e) {
		}

		sleep(rdbDumpMinIntervalMilli * 2);
		redisKeeperServer.setRdbDumper(dump1);
	}

	@Test
	public void testKeeperStopNoConnectMaster() throws Exception {
		FakeRedisServer server1 = startFakeRedisServer();
		FakeRedisServer server2 = startFakeRedisServer();
		FakeRedisServer server3 = startFakeRedisServer();

		RedisKeeperServer redisKeeperServer = createRedisKeeperServer();

		redisKeeperServer.initialize();
		redisKeeperServer.start();

		redisKeeperServer.setRedisKeeperServerState(
				new RedisKeeperServerStateActive(redisKeeperServer, localHostEndpoint(server1.getPort())));
		redisKeeperServer.reconnectMaster();

		waitConditionUntilTimeOut(() -> server1.getConnected() == 1);

		sleep(100);
		redisKeeperServer.stop();

		redisKeeperServer.setRedisKeeperServerState(
				new RedisKeeperServerStateActive(redisKeeperServer, localHostEndpoint(server2.getPort())));
		redisKeeperServer.reconnectMaster();

		waitConditionUntilTimeOut(() -> server1.getConnected() == 0);
		Assert.assertEquals(0, server2.getConnected());

		redisKeeperServer.dispose();

		redisKeeperServer.setRedisKeeperServerState(
				new RedisKeeperServerStateActive(redisKeeperServer, localHostEndpoint(server3.getPort())));
		redisKeeperServer.reconnectMaster();
		sleep(100);
		Assert.assertEquals(0, server1.getConnected());
		Assert.assertEquals(0, server2.getConnected());
		Assert.assertEquals(0, server3.getConnected());
	}

	@Test
	public void testKeeperServerInitState() throws Exception {

		KeeperMeta keeperMeta = createKeeperMeta();
		ReplId replId = getReplId();

		RedisKeeperServer redisKeeperServer = createRedisKeeperServer(replId.id(), keeperMeta);
		redisKeeperServer.initialize();

		Assert.assertEquals(KeeperState.UNKNOWN, redisKeeperServer.getRedisKeeperServerState().keeperState());

		redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateActive(redisKeeperServer));
		redisKeeperServer.getReplicationStore().getMetaStore().becomeActive();
		redisKeeperServer.dispose();


		redisKeeperServer = createRedisKeeperServer(replId.id(), keeperMeta);
		redisKeeperServer.initialize();
		Assert.assertEquals(KeeperState.PRE_ACTIVE, redisKeeperServer.getRedisKeeperServerState().keeperState());

		redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateBackup(redisKeeperServer));
		redisKeeperServer.getReplicationStore().getMetaStore().becomeBackup();
		redisKeeperServer.dispose();


		redisKeeperServer = createRedisKeeperServer(replId.id(), keeperMeta);
		redisKeeperServer.initialize();
		Assert.assertEquals(KeeperState.PRE_BACKUP, redisKeeperServer.getRedisKeeperServerState().keeperState());
	}

	@Ignore
	@Test
	public void manuallyTestKeeperStats() throws Exception {

		RedisKeeperServer redisKeeperServer = createRedisKeeperServer();
		logger.info("[listening-port] {}", redisKeeperServer.getListeningPort());
		sleep(1000 * 30);
		redisKeeperServer.initialize();
		redisKeeperServer.start();
		sleep(1000 * 60 * 60);
	}

	@Override
	protected String getXpipeMetaConfigFile() {
		return "keeper-test.xml";
	}

	@Test
	public void fixDeadSlave() throws Exception {

		DefaultRedisKeeperServer redisKeeperServer = (DefaultRedisKeeperServer) createRedisKeeperServer();
		Channel channel = new EmbeddedChannel();
		RedisClient client = redisKeeperServer.clientConnected(channel);
		redisKeeperServer.clientDisconnected(channel);
		RedisSlave slave = client.becomeSlave();
		assertFalse(redisKeeperServer.allClients().contains(slave));
	}

	private RedisSlave mockRedisSlave(RedisKeeperServer redisKeeperServer) {
		ChannelFuture future = Mockito.mock(ChannelFuture.class);
		Channel channel = Mockito.mock(Channel.class);
		when(channel.closeFuture()).thenReturn(future);
		RedisClient client =  redisKeeperServer.clientConnected(channel);
		RedisSlave slave = client.becomeSlave();
		return slave;
	}

	@Test
	public void dumpExecuteFailClearsRdbDumperSoNextSlaveCanDump() throws Exception {
		RedisKeeperServer redisKeeperServer = createRedisKeeperServer();
		redisKeeperServer.initialize();

		RedisMasterNewRdbDumper failingDumper = spy(new RedisMasterNewRdbDumper(
				mock(RedisMaster.class), redisKeeperServer, false, false,
				mock(NioEventLoopGroup.class), mock(ScheduledExecutorService.class),
				mock(KeeperResourceManager.class)));
		doThrow(new IOException("prepareNewRdb fail")).when(failingDumper).startRdbOnlyReplication();

		redisKeeperServer.setRdbDumper(failingDumper);
		failingDumper.execute();

		assertNull(redisKeeperServer.rdbDumper());
		assertTrue(failingDumper.future().isDone());
		assertFalse(failingDumper.future().isSuccess());

		RdbDumper nextDumper = mock(RdbDumper.class);
		when(nextDumper.tryRordb()).thenReturn(false);
		redisKeeperServer.setRdbDumper(nextDumper);

		RedisSlave slave2 = mock(RedisSlave.class);
		redisKeeperServer.fullSyncToSlave(slave2);

		verify(nextDumper).tryFullSync(slave2);
		verify(slave2, never()).waitForRdbDumping();
	}

	@Test
	public void testReqFsyncSeq() throws Exception {
		((TestKeeperConfig)keeperConfig).setMaxLoadingSlaves(1);
		RedisKeeperServer redisKeeperServer = createRedisKeeperServer();
		redisKeeperServer.initialize();
		redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateActive(
				redisKeeperServer, new DefaultEndPoint("10.0.0.1", 6379, Mockito.mock(ProxyConnectProtocol.class))));
		redisKeeperServer.reconnectMaster();
		RdbDumper dumper = Mockito.mock(RdbDumper.class);
		redisKeeperServer.setRdbDumper(dumper);

		RedisSlave slave1 = mockRedisSlave(redisKeeperServer);
		RedisSlave slave2 = mockRedisSlave(redisKeeperServer);

		redisKeeperServer.fullSyncToSlave(slave1);
		redisKeeperServer.fullSyncToSlave(slave2);
		Assert.assertEquals(slave2.getSlaveState(), REDIS_REPL_WAIT_SEQ_FSYNC);

		slave1.close();
		redisKeeperServer.clientDisconnected(slave1.channel());
		((DefaultRedisKeeperServer)redisKeeperServer).updateLoadingSlaves();
		((DefaultRedisKeeperServer)redisKeeperServer).continueFsyncSequentially();
		waitConditionUntilTimeOut(() -> {
			try {
				verify(dumper, times(2)).tryFullSync(any());
				return true;
			} catch (Throwable e) {
				return false;
			}
		});
	}

}

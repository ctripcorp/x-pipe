package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.proxy.protocols.DefaultProxyConnectProtocol;
import com.ctrip.xpipe.redis.core.server.FakeRedisServer;
import com.ctrip.xpipe.redis.keeper.*;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
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

		redisKeeperServer.stop();
		redisKeeperServer.getReplicationStore();

		redisKeeperServer.dispose();

		logger.info("after dispose");
		try{
			redisKeeperServer.getReplicationStore();
			Assert.fail();
		}catch (Exception e){
			logger.info("{}", e);
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

		redisKeeperServer.clearRdbDumper(dump1);

		// too quick
		// force can success
		redisKeeperServer.setRdbDumper(dump1, true);
		redisKeeperServer.clearRdbDumper(dump1);

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

		RedisKeeperServer redisKeeperServer = createRedisKeeperServer(keeperMeta);
		redisKeeperServer.initialize();

		Assert.assertEquals(KeeperState.UNKNOWN, redisKeeperServer.getRedisKeeperServerState().keeperState());

		redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateActive(redisKeeperServer));
		redisKeeperServer.getReplicationStore().getMetaStore().becomeActive();
		redisKeeperServer.dispose();


		redisKeeperServer = createRedisKeeperServer(keeperMeta);
		redisKeeperServer.initialize();
		Assert.assertEquals(KeeperState.PRE_ACTIVE, redisKeeperServer.getRedisKeeperServerState().keeperState());

		redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateBackup(redisKeeperServer));
		redisKeeperServer.getReplicationStore().getMetaStore().becomeBackup();
		redisKeeperServer.dispose();


		redisKeeperServer = createRedisKeeperServer(keeperMeta);
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

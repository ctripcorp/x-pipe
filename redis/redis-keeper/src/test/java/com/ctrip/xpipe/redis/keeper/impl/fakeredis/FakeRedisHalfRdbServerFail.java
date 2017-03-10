package com.ctrip.xpipe.redis.keeper.impl.fakeredis;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.redis.core.protocal.cmd.InMemoryPsync;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;

/**
 * @author wenchao.meng
 *
 *         Mar 9, 2017
 */
public class FakeRedisHalfRdbServerFail extends AbstractFakeRedisTest {
	
	private RedisKeeperServer redisKeeperServer;
	private int dumpMinIntervalMilli = 60000; 

	@Before
	public void beforeFakeRedisHalfRdbServerFail() throws Exception {
		
		fakeRedisServer.setSendHalfRdbAndCloseConnectionCount(1);
		redisKeeperServer = startRedisKeeperServerAndConnectToFakeRedis();
		
		TestKeeperConfig testKeeperConfig = (TestKeeperConfig) redisKeeperServer.getKeeperConfig();
		testKeeperConfig.setRdbDumpMinIntervalMilli(dumpMinIntervalMilli);
		sleep(1500);
	}

	@Test
	public void redisFailWhileSendingRdb() throws Exception {


		InMemoryPsync inMemoryPsync = sendInmemoryPsync("localhost", redisKeeperServer.getListeningPort());
		try {
			inMemoryPsync.future().get(3, TimeUnit.SECONDS);
			Assert.fail();
		} catch (TimeoutException e) {
			Assert.fail();
		} catch (Exception e) {
			logger.error("[redisFailWhileSendingRdb]");
		}

		inMemoryPsync = sendInmemoryPsync("localhost", redisKeeperServer.getListeningPort());

		sleep(1000);
		assertPsyncResultEquals(inMemoryPsync);

	}

	@Test
	public void redisFailKeeperRestartDumpNewRdb() throws Exception {
		redisKeeperServer.stop();
		redisKeeperServer.dispose();

		redisKeeperServer.initialize();
		redisKeeperServer.start();
		
		connectToFakeRedis(redisKeeperServer);
		sleep(1000);
		InMemoryPsync inMemoryPsync = sendInmemoryPsync("localhost", redisKeeperServer.getListeningPort());
		sleep(1500);
		assertPsyncResultEquals(inMemoryPsync);
		Assert.assertEquals(1, redisKeeperServer.getKeeperMonitor().getReplicationStoreStats().getReplicationStoreCreateCount());
	}
	
	
	@After
	public void afterFakeRedisHalfRdbServerFail() throws IOException {
		waitForAnyKeyToExit();
	}

}

package com.ctrip.xpipe.redis.keeper.impl.fakeredis;

import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.cmd.InMemoryPsync;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeoutException;

/**
 * @author wenchao.meng
 *
 *         Mar 9, 2017
 */
public class FakeRedisHalfRdbServerFail extends AbstractFakeRedisTest {

	private RedisKeeperServer redisKeeperServer;
	private int dumpMinIntervalMilli = 60000;
	private int sleepBeforeSendFullSyncInfo = 200;

	@Before
	public void beforeFakeRedisHalfRdbServerFail() throws Exception {

		fakeRedisServer.setSendHalfRdbAndCloseConnectionCount(1);
		fakeRedisServer.setSleepBeforeSendFullSyncInfo(sleepBeforeSendFullSyncInfo);
		redisKeeperServer = startRedisKeeperServerAndConnectToFakeRedis();

		TestKeeperConfig testKeeperConfig = (TestKeeperConfig) redisKeeperServer.getKeeperConfig();
		testKeeperConfig.setRdbDumpMinIntervalMilli(dumpMinIntervalMilli);
	}

	@Test
	public void redisFailWhileSendingRdb() throws Exception {

		waitUntilRedisMasterConnected(redisKeeperServer);

		logger.info(remarkableMessage("[redisFailWhileSendingRdb]"));
		InMemoryPsync inMemoryPsync = sendInmemoryPsync("localhost", redisKeeperServer.getListeningPort());

		sleep(1500);
		assertPsyncResultEquals(inMemoryPsync);

	}

	private void waitUntilRedisMasterConnected(RedisKeeperServer redisKeeperServer) throws TimeoutException {
		waitUntilRedisMasterConnected(redisKeeperServer, 10000);
	}

	private void waitUntilRedisMasterConnected(RedisKeeperServer redisKeeperServer, int timeOutMilli) throws TimeoutException {

		long begin = System.currentTimeMillis();
		while (true) {

			if (redisKeeperServer.getRedisMaster().getMasterState() == MASTER_STATE.REDIS_REPL_CONNECTED) {
				return;
			}
			if(System.currentTimeMillis() - begin > timeOutMilli){
				throw new TimeoutException("timeout:" + timeOutMilli);
			}
			sleep(2);
		}
	}

	@Test
	public void redisFailKeeperRestartDumpNewRdb() throws Exception {

		sleep(sleepBeforeSendFullSyncInfo + 1000);

		redisKeeperServer.stop();
		redisKeeperServer.dispose();

		redisKeeperServer.initialize();
		redisKeeperServer.start();

		connectToFakeRedis(redisKeeperServer);
		
		waitUntilRedisMasterConnected(redisKeeperServer);
		sleep(1000);

		InMemoryPsync inMemoryPsync = sendInmemoryPsync("localhost", redisKeeperServer.getListeningPort());
		sleep(1500);
		assertPsyncResultEquals(inMemoryPsync);
		Assert.assertEquals(1,
				redisKeeperServer.getKeeperMonitor().getReplicationStoreStats().getReplicationStoreCreateCount());
	}
}

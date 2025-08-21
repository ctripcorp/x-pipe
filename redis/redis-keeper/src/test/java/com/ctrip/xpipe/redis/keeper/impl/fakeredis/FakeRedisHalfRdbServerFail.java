package com.ctrip.xpipe.redis.keeper.impl.fakeredis;

import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.cmd.InMemoryPsync;
import com.ctrip.xpipe.redis.core.utils.SimplePsyncObserver;
import com.ctrip.xpipe.redis.keeper.AbstractFakeRedisTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

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

		waitConditionUntilTimeOut(() -> {return redisKeeperServer.getRedisMaster().getMasterState() == MASTER_STATE.REDIS_REPL_CONNECTED;});

		logger.info(remarkableMessage("[redisFailWhileSendingRdb]"));
		InMemoryPsync inMemoryPsync = sendInmemoryPsync("localhost", redisKeeperServer.getListeningPort());

		sleep(3000);
		assertPsyncResultEquals(inMemoryPsync);

	}

	@Test
	public void redisFailKeeperRestartDumpNewRdb() throws Exception {

		sleep(sleepBeforeSendFullSyncInfo + 1000);

		redisKeeperServer.stop();
		redisKeeperServer.dispose();

		redisKeeperServer.initialize();
		redisKeeperServer.start();

		connectToFakeRedis(redisKeeperServer);

		waitConditionUntilTimeOut(() -> {return redisKeeperServer.getRedisMaster().getMasterState() == MASTER_STATE.REDIS_REPL_CONNECTED;});

		SimplePsyncObserver simplePsyncObserver = new SimplePsyncObserver();
		InMemoryPsync inMemoryPsync = sendInmemoryPsync("localhost", redisKeeperServer.getListeningPort(), simplePsyncObserver);
		//wait
		simplePsyncObserver.getOnline().get(8000, TimeUnit.MILLISECONDS);
		//wait for commands
		sleep(1000);

		assertPsyncResultEquals(inMemoryPsync);
		Assert.assertEquals(1,
				redisKeeperServer.getKeeperMonitor().getReplicationStoreStats().getReplicationStoreCreateCount());
	}
}

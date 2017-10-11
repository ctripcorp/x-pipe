package com.ctrip.xpipe.redis.keeper.impl.fakeredis;

import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.impl.AbstractRedisMasterReplication;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 *
 *         2016年4月21日 下午5:42:29
 */
public class FakeRedisRdbDumpLong extends AbstractFakeRedisTest {

	private int replicationTimeout = 1;

	@Override
	public void beforeAbstractTest() throws Exception {
		super.beforeAbstractTest();
		AbstractRedisMasterReplication.DEFAULT_REPLICATION_TIMEOUT = replicationTimeout;
	}

	@Test
	public void testRedisWithLf() throws Exception {

		int sleepBeforeSendRdb = replicationTimeout * 2000;
		fakeRedisServer.setSleepBeforeSendRdb(sleepBeforeSendRdb);

		RedisKeeperServer redisKeeperServer = startRedisKeeperServerAndConnectToFakeRedis();

		waitConditionUntilTimeOut(
				() -> MASTER_STATE.REDIS_REPL_CONNECTED == redisKeeperServer.getRedisMaster().getMasterState(),
				replicationTimeout * 5000
		);

	}

	@Test
	public void testRedisNoLf() throws Exception {

		int sleepBeforeSendRdb = replicationTimeout * 3000;
		fakeRedisServer.setSleepBeforeSendRdb(sleepBeforeSendRdb);
		fakeRedisServer.setSendLFBeforeSendRdb(false);

		RedisKeeperServer redisKeeperServer = startRedisKeeperServerAndConnectToFakeRedis();

		sleep(replicationTimeout * 3000);

		Assert.assertEquals(MASTER_STATE.REDIS_REPL_HANDSHAKE, redisKeeperServer.getRedisMaster().getMasterState());

	}

}

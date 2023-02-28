package com.ctrip.xpipe.redis.keeper.impl.fakeredis;

import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.keeper.AbstractFakeRedisTest;
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

	private int replicationTimeoutMilli = 200;
	private int BACK_DEFAULT_REPLICATION_TIMEOUT_MILLI;

	@Override
	public void beforeAbstractTest() throws Exception {
		super.beforeAbstractTest();
		BACK_DEFAULT_REPLICATION_TIMEOUT_MILLI = AbstractRedisMasterReplication.DEFAULT_REPLICATION_TIMEOUT_MILLI;
		AbstractRedisMasterReplication.DEFAULT_REPLICATION_TIMEOUT_MILLI = replicationTimeoutMilli;
	}

	@Override
	public void afterAbstractTest() throws Exception {
		AbstractRedisMasterReplication.DEFAULT_REPLICATION_TIMEOUT_MILLI = BACK_DEFAULT_REPLICATION_TIMEOUT_MILLI;
		super.afterAbstractTest();
	}

	@Test
	public void testRedisWithLf() throws Exception {

		int sleepBeforeSendRdb = replicationTimeoutMilli * 2;
		fakeRedisServer.setSleepBeforeSendRdb(sleepBeforeSendRdb);

		RedisKeeperServer redisKeeperServer = startRedisKeeperServerAndConnectToFakeRedis();

		waitConditionUntilTimeOut(
				() -> MASTER_STATE.REDIS_REPL_CONNECTED == redisKeeperServer.getRedisMaster().getMasterState(),
				replicationTimeoutMilli * 5
		);

	}

	@Test
	public void testRedisNoLf() throws Exception {

		int sleepBeforeSendRdb = replicationTimeoutMilli * 3;
		fakeRedisServer.setSleepBeforeSendRdb(sleepBeforeSendRdb);
		fakeRedisServer.setSendLFBeforeSendRdb(false);

		RedisKeeperServer redisKeeperServer = startRedisKeeperServerAndConnectToFakeRedis(100, allCommandsSize, 1000, 200);

		sleep(replicationTimeoutMilli * 3);

		Assert.assertEquals(MASTER_STATE.REDIS_REPL_NONE, redisKeeperServer.getRedisMaster().getMasterState());

	}

}

package com.ctrip.xpipe.redis.keeper.impl.fakeredis;

import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.cmd.InMemoryGapAllowedSync;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.AbstractFakeRedisTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author wenchao.meng
 *
 *         2016年4月21日 下午5:42:29
 */
public class DefaultRedisKeeperServerConnectToFakeRedisTest extends AbstractFakeRedisTest {

	@Before
	public void setupDefaultRedisKeeperServerConnectToFakeRedisTest() {
		this.fakeRedisServer.setRdbSize(10000);
	}

	@Test
	public void testReplicationData() throws Exception{

		RedisKeeperServer redisKeeperServer = startRedisKeeperServerAndConnectToFakeRedis();
		sleep(1500);

		logger.info(remarkableMessage("[testReplicationData][read replication store]"));

		ReplicationStore replicationStore = redisKeeperServer.getReplicationStore();
		byte[] rdbContent = readRdbFileTilEnd(replicationStore);
		Assert.assertArrayEquals(fakeRedisServer.getRdbContent(), rdbContent);

		String commands = readCommandFileTilEnd(replicationStore, fakeRedisServer.currentCommands().length());
		Assert.assertEquals(fakeRedisServer.currentCommands(), commands);
	}

	@Test
	public void testNewDumpCommandsLost() throws Exception{

		startKeeperServerAndTestReFullSync(1, allCommandsSize);
	}

	@Test
	public void testNewDumpCommandsTooMush() throws Exception{

		startKeeperServerAndTestReFullSync(100, (int) (allCommandsSize * 0.8));
	}

	private void startKeeperServerAndTestReFullSync(int fileToKeep, int maxTransferCommnadsSize) throws Exception {

		RedisKeeperServer redisKeeperServer = startRedisKeeperServerAndConnectToFakeRedis(fileToKeep, maxTransferCommnadsSize, 1000);
		int keeperPort = redisKeeperServer.getListeningPort();
		sleep(5000);
		logger.info(remarkableMessage("send psync to redump rdb"));

		int rdbDumpCount1 = ((DefaultReplicationStore)redisKeeperServer.getReplicationStore()).getRdbUpdateCount();

		InMemoryGapAllowedSync gasync = sendInmemoryGAsync("localhost", keeperPort);
		waitConditionUntilTimeOut(() -> gasync.getCommands().length >= fakeRedisServer.getCommandsLength());
		int rdbDumpCount2 = ((DefaultReplicationStore)redisKeeperServer.getReplicationStore()).getRdbUpdateCount();
		Assert.assertEquals(rdbDumpCount1 + 1, rdbDumpCount2);

		assertGAsyncResultEquals(gasync);
	}


	@Test
	public void testDumpWhileWaitForRdb() throws Exception{

		int sleepBeforeSendRdb = 2000;

		fakeRedisServer.setSleepBeforeSendRdb(sleepBeforeSendRdb);
		RedisKeeperServer redisKeeperServer = startRedisKeeperServerAndConnectToFakeRedis(100, allCommandsSize);
		waitConditionUntilTimeOut(() -> redisKeeperServer.getRedisMaster().getMasterState().equals(MASTER_STATE.REDIS_REPL_CONNECTED));

		sleep(sleepBeforeSendRdb/4);
		int rdbDumpCount1 = ((DefaultRedisKeeperServer)redisKeeperServer).getRdbDumpTryCount();

		Assert.assertEquals(1, rdbDumpCount1);

		int keeperPort = redisKeeperServer.getListeningPort();
		logger.info(remarkableMessage("send psync to keeper port:{}"), keeperPort);

		InMemoryGapAllowedSync gasync = sendInmemoryGAsync("localhost", keeperPort, "?", -1L);

		sleep(1000);
		int rdbDumpCount2 = ((DefaultRedisKeeperServer)redisKeeperServer).getRdbDumpTryCount();
		Assert.assertEquals(1, rdbDumpCount2);

		sleep(sleepBeforeSendRdb);

		waitForGAsyncResultEquals(gasync);
	}

}


package com.ctrip.xpipe.redis.keeper.impl;

import java.net.InetSocketAddress;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.redis.core.protocal.cmd.InMemoryPsync;
import com.ctrip.xpipe.redis.core.server.FakeRedisServer;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperContextTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;

/**
 * @author wenchao.meng
 *
 *         2016年4月21日 下午5:42:29
 */
public class DefaultRedisKeeperServerConnectToFakeRedisTest extends AbstractRedisKeeperContextTest {
	
	private FakeRedisServer fakeRedisServer;
	private RedisKeeperServer redisKeeperServer;
	
	private int commandFileSize;
	private int allCommandsSize; 
	
	@Before
	public void beforeDefaultRedisKeeperServerTest() throws Exception {
		fakeRedisServer = startFakeRedisServer();
		allCommandsSize = fakeRedisServer.getCommandsLength();
		commandFileSize = fakeRedisServer.getSendBatchSize();

	}

	protected void startRedisKeeperServerAndConnectToFakeRedis( ) throws Exception {
		startRedisKeeperServerAndConnectToFakeRedis(100, allCommandsSize);
	}

	protected void startRedisKeeperServerAndConnectToFakeRedis(int replicationStoreCommandFileNumToKeep, 
			int replicationStoreMaxCommandsToTransferBeforeCreateRdb) throws Exception {
		startRedisKeeperServerAndConnectToFakeRedis(replicationStoreCommandFileNumToKeep, replicationStoreMaxCommandsToTransferBeforeCreateRdb, 1000);
	}

	private void startRedisKeeperServerAndConnectToFakeRedis(int replicationStoreCommandFileNumToKeep, 
			int replicationStoreMaxCommandsToTransferBeforeCreateRdb, int minTimeMilliToGcAfterCreate) throws Exception {
		
		KeeperConfig keeperConfig = new TestKeeperConfig(
				commandFileSize, 
				replicationStoreCommandFileNumToKeep, 
				replicationStoreMaxCommandsToTransferBeforeCreateRdb, minTimeMilliToGcAfterCreate);
		redisKeeperServer = createRedisKeeperServer(keeperConfig);
		redisKeeperServer.initialize();
		redisKeeperServer.start();
		add(redisKeeperServer);
		redisKeeperServer.getRedisKeeperServerState().becomeActive(new InetSocketAddress("localhost", fakeRedisServer.getPort()));
	}

	@Test
	public void testReplicationData() throws Exception{
		
		startRedisKeeperServerAndConnectToFakeRedis();
		sleep(1500);
		
		logger.info(remarkableMessage("[testReplicationData][read replication store]"));	
		
		ReplicationStore replicationStore = redisKeeperServer.getReplicationStore();
		String rdbContent = readRdbFileTilEnd(replicationStore);
		Assert.assertEquals(fakeRedisServer.getRdbContent(), rdbContent);

		String commands = readCommandFileTilEnd(replicationStore);
		Assert.assertEquals(fakeRedisServer.currentCommands(), commands);
	}
	
	@Test
	public void testNewDumpCommandsLost() throws Exception{

		startKeeperServerAndTestReFullSync(2, allCommandsSize);
	}

	private void startKeeperServerAndTestReFullSync(int fileToKeep, int maxTransferCommnadsSize) throws Exception {
		
		startRedisKeeperServerAndConnectToFakeRedis(fileToKeep, maxTransferCommnadsSize, 1000);
		int keeperPort = redisKeeperServer.getListeningPort();
		sleep(2000);
		logger.info(remarkableMessage("send psync to redump rdb"));
		
		int rdbDumpCount1 = ((DefaultReplicationStore)redisKeeperServer.getReplicationStore()).getRdbUpdateCount();
		InMemoryPsync psync = new InMemoryPsync("localhost", keeperPort, "?", -1L);
		psync.execute();
		sleep(1000);
		int rdbDumpCount2 = ((DefaultReplicationStore)redisKeeperServer.getReplicationStore()).getRdbUpdateCount();
		Assert.assertEquals(rdbDumpCount2, rdbDumpCount1 + 1);
		
		assertPsyncResultEquals(psync);
	}

	@Test
	public void testNewDumpCommandsTooMush() throws Exception{
		
		startKeeperServerAndTestReFullSync(100, commandFileSize);
	}

	@Test
	public void testDumpWhileWaitForRdb() throws Exception{
		
		int sleepBeforeSendRdb = 2000;
		
		fakeRedisServer.setSleepBeforeSendRdb(sleepBeforeSendRdb);
		startRedisKeeperServerAndConnectToFakeRedis(100, allCommandsSize);
		
		sleep(sleepBeforeSendRdb/4);
		int rdbDumpCount1 = ((DefaultRedisKeeperServer)redisKeeperServer).getRdbDumpTryCount();
		Assert.assertEquals(1, rdbDumpCount1);
		
		int keeperPort = redisKeeperServer.getListeningPort();
		logger.info(remarkableMessage("send psync to keeper port:{}"), keeperPort);
		InMemoryPsync psync = new InMemoryPsync("localhost", keeperPort, "?", -1L);
		psync.execute();
		sleep(1000);
		int rdbDumpCount2 = ((DefaultRedisKeeperServer)redisKeeperServer).getRdbDumpTryCount();
		Assert.assertEquals(1, rdbDumpCount2);
		
		sleep(sleepBeforeSendRdb);
		
		assertPsyncResultEquals(psync);
	}
	
	private void assertPsyncResultEquals(InMemoryPsync psync) {
		Assert.assertEquals(fakeRedisServer.getRdbContent(), new String(psync.getRdb()));
		Assert.assertEquals(fakeRedisServer.currentCommands(), new String(psync.getCommands()));
	}

	@Override
	protected String getXpipeMetaConfigFile() {
		return "keeper-test.xml";
	}
}

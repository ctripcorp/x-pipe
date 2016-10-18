package com.ctrip.xpipe.redis.keeper.impl.fakeredis;

import java.net.InetSocketAddress;

import org.junit.Assert;
import org.junit.Before;

import com.ctrip.xpipe.redis.core.protocal.cmd.InMemoryPsync;
import com.ctrip.xpipe.redis.core.server.FakeRedisServer;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperContextTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;

/**
 * @author wenchao.meng
 *
 * Oct 18, 2016
 */
public class AbstractFakeRedisTest extends AbstractRedisKeeperContextTest{
	
	protected FakeRedisServer fakeRedisServer;

	protected int commandFileSize;
	protected int allCommandsSize; 

	@Before
	public void beforeAbstractFakeRedisTest() throws Exception{
		fakeRedisServer = startFakeRedisServer();
		
		allCommandsSize = fakeRedisServer.getCommandsLength();
		commandFileSize = fakeRedisServer.getSendBatchSize();
	}
	
	
	
	protected RedisKeeperServer startRedisKeeperServerAndConnectToFakeRedis() throws Exception {
		return startRedisKeeperServerAndConnectToFakeRedis(100, allCommandsSize);
	}


	protected RedisKeeperServer startRedisKeeperServerAndConnectToFakeRedis(int replicationStoreCommandFileNumToKeep, 
			int replicationStoreMaxCommandsToTransferBeforeCreateRdb) throws Exception {
		return startRedisKeeperServerAndConnectToFakeRedis(replicationStoreCommandFileNumToKeep, replicationStoreMaxCommandsToTransferBeforeCreateRdb, 1000);
	}

	protected RedisKeeperServer startRedisKeeperServerAndConnectToFakeRedis(int replicationStoreCommandFileNumToKeep, 
			int replicationStoreMaxCommandsToTransferBeforeCreateRdb, int minTimeMilliToGcAfterCreate) throws Exception {

		RedisKeeperServer redisKeeperServer = startRedisKeeperServer(replicationStoreCommandFileNumToKeep, replicationStoreMaxCommandsToTransferBeforeCreateRdb, 1000);
		connectToRedis(redisKeeperServer);
		return redisKeeperServer;
	}

	

	protected RedisKeeperServer startRedisKeeperServer() throws Exception {
		return startRedisKeeperServer(100, allCommandsSize, 1000);
	}

	protected RedisKeeperServer startRedisKeeperServer(int replicationStoreCommandFileNumToKeep, 
			int replicationStoreMaxCommandsToTransferBeforeCreateRdb, int minTimeMilliToGcAfterCreate) throws Exception {
		
		KeeperConfig keeperConfig = new TestKeeperConfig(
				commandFileSize, 
				replicationStoreCommandFileNumToKeep, 
				replicationStoreMaxCommandsToTransferBeforeCreateRdb, minTimeMilliToGcAfterCreate);
		RedisKeeperServer redisKeeperServer = createRedisKeeperServer(keeperConfig);
		redisKeeperServer.initialize();
		redisKeeperServer.start();
		add(redisKeeperServer);
		
		return redisKeeperServer;
	}

	private void connectToRedis(RedisKeeperServer redisKeeperServer) {
		redisKeeperServer.getRedisKeeperServerState().becomeActive(new InetSocketAddress("localhost", fakeRedisServer.getPort()));
		
	}

	protected void assertPsyncResultEquals(InMemoryPsync psync) {

		try{
			Assert.assertEquals(fakeRedisServer.getRdbContent(), new String(psync.getRdb()));
			Assert.assertEquals(fakeRedisServer.currentCommands(), new String(psync.getCommands()));
		}catch(Exception e){
			logger.error("[assertPsyncResultEquals]", e);
		}

		Assert.assertEquals(fakeRedisServer.getRdbContent(), new String(psync.getRdb()));
		Assert.assertEquals(fakeRedisServer.currentCommands(), new String(psync.getCommands()));
	}

	@Override
	protected String getXpipeMetaConfigFile() {
		return "keeper-test.xml";
	}
}

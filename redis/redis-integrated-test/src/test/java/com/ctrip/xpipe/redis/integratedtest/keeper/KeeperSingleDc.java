package com.ctrip.xpipe.redis.integratedtest.keeper;


import java.io.IOException;

import org.apache.commons.exec.ExecuteException;
import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;


/**
 * @author wenchao.meng
 *
 * Aug 17, 2016
 */
public class KeeperSingleDc extends AbstractKeeperIntegratedSingleDc{
	
	private int replicationStoreCommandFileSize = 1024;
	private int replicationStoreCommandFileNumToKeep = 2;
	private int replicationStoreMaxCommandsToTransferBeforeCreateRdb = 1024;
	
	@Test
	public void testSync() throws IOException{

		sendMessageToMasterAndTestSlaveRedis();
	}
	
	@Test
	public void testMakeBackupActive() throws Exception{
		
		RedisMeta redisMaster = getRedisMaster();
		KeeperMeta backupKeeper = getKeepersBackup().get(0);
		RedisKeeperServer redisKeeperServer = getRedisKeeperServer(backupKeeper);
		Assert.assertEquals(PARTIAL_STATE.FULL, redisKeeperServer.getRedisMaster().partialState());
		
		logger.info(remarkableMessage("make keeper active{}"), backupKeeper);
		setKeeperState(backupKeeper, KeeperState.ACTIVE, redisMaster.getIp(), redisMaster.getPort());
		
		sleep(2000);
		Assert.assertEquals(PARTIAL_STATE.PARTIAL, redisKeeperServer.getRedisMaster().partialState());
		
		
		//make sure keeper works
		sendMessageToMaster();
		RedisMeta newSlave = createSlave(backupKeeper.getIp(), backupKeeper.getPort());
		startRedis(getDcMeta(), newSlave);
		
		assertRedisEquals();
	}

	@Test
	public void testMakeActiveBackup() throws Exception{

		RedisMeta redisMaster = getRedisMaster();
		
		KeeperMeta backupKeeper = getKeepersBackup().get(0);
		logger.info(remarkableMessage("make keeper active{}"), backupKeeper);
		setKeeperState(backupKeeper, KeeperState.ACTIVE, redisMaster.getIp(), redisMaster.getPort());

		
		KeeperMeta activeKeeper = getKeeperActive();
		RedisKeeperServer redisKeeperServer = getRedisKeeperServer(activeKeeper);
		
		Assert.assertEquals(PARTIAL_STATE.FULL, redisKeeperServer.getRedisMaster().partialState());
		logger.info(remarkableMessage("[testMakeActiveBackup]{}"), activeKeeper);
		setKeeperState(activeKeeper, KeeperState.BACKUP, backupKeeper.getIp(), backupKeeper.getPort());
		sleep(2000);
		Assert.assertEquals(PARTIAL_STATE.PARTIAL, redisKeeperServer.getRedisMaster().partialState());

	}

	@Test
	public void testReFullSync() throws ExecuteException, IOException{
		
		RedisMeta redisMaster = getRedisMaster();
		RedisKeeperServer redisKeeperServer = getRedisKeeperServerActive(dc);
		DefaultReplicationStore replicationStore = (DefaultReplicationStore) redisKeeperServer.getReplicationStore();
		

		DcMeta dcMeta = getDcMeta();
		KeeperMeta activeKeeper = getKeeperActive();

		RedisMeta slave1 = createSlave(activeKeeper.getIp(), activeKeeper.getPort());

		int lastRdbUpdateCount = replicationStore.getRdbUpdateCount();
		logger.info(remarkableMessage("[testReFullSync][sendRandomMessage]"));
		sendRandomMessage(redisMaster, 1, replicationStoreCommandFileSize);
		
		for(int i=0;i<5;i++){
			
			logger.info(remarkableMessage("[testReFullSync]{}"), i);
			startRedis(dcMeta, slave1);
			sleep(5000);
			int currentRdbUpdateCount = replicationStore.getRdbUpdateCount();
			logger.info("[testReFullSync]{},{}", lastRdbUpdateCount, currentRdbUpdateCount);
			Assert.assertEquals(lastRdbUpdateCount + 1, currentRdbUpdateCount);
			lastRdbUpdateCount = currentRdbUpdateCount;
			sendMesssageToMasterAndTest(100, slave1);
		}
		
	}

	private RedisMeta createSlave(String masterIp, Integer masterPort) {
		
		RedisMeta slave = new RedisMeta();
		slave.setMaster(String.format("%s:%d", masterIp, masterPort));
		slave.setPort(randomPort());		
		return slave;
	}


	@Override
	protected KeeperConfig getKeeperConfig() {
		return new TestKeeperConfig(replicationStoreCommandFileSize, replicationStoreCommandFileNumToKeep, replicationStoreMaxCommandsToTransferBeforeCreateRdb);
	}
}

package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import com.google.common.collect.Lists;
import org.apache.commons.exec.ExecuteException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * Aug 17, 2016
 */
public class KeeperSingleDc extends AbstractKeeperIntegratedSingleDc{
	
	@Test
	public void testSignleKeeperSync() throws IOException{

		sendMessageToMasterAndTestSlaveRedis();
	}
	
	@Test
	public void testMakeBackupActive() throws Exception{
		
		RedisKeeperServer redisKeeperServer = getRedisKeeperServer(backupKeeper);
		Assert.assertEquals(PARTIAL_STATE.PARTIAL, redisKeeperServer.getRedisMaster().partialState());
		
		logger.info(remarkableMessage("make keeper active{}"), backupKeeper);
		setKeeperState(backupKeeper, KeeperState.ACTIVE, redisMaster.getIp(), redisMaster.getPort());
		
		sleep(2000);
		Assert.assertEquals(PARTIAL_STATE.PARTIAL, redisKeeperServer.getRedisMaster().partialState());
	}

	@Test
	public void testMakeActiveBackup() throws Exception{

		logger.info(remarkableMessage("make keeper active{}"), backupKeeper);
		setKeeperState(backupKeeper, KeeperState.ACTIVE, redisMaster.getIp(), redisMaster.getPort());

		
		RedisKeeperServer redisKeeperServer = getRedisKeeperServer(activeKeeper);
		
		Assert.assertEquals(PARTIAL_STATE.FULL, redisKeeperServer.getRedisMaster().partialState());
		logger.info(remarkableMessage("[testMakeActiveBackup]{}"), activeKeeper);
		setKeeperState(activeKeeper, KeeperState.BACKUP, backupKeeper.getIp(), backupKeeper.getPort());
		sleep(2000);
		Assert.assertEquals(PARTIAL_STATE.PARTIAL, redisKeeperServer.getRedisMaster().partialState());

	}

	@Test
	public void testBackupActiveChangeManyTimes() throws Exception{
				
		for(int i=0;i<3;i++){
			
			logger.info(remarkableMessage("------{}-------"), i);
			//exchange role
			//make backup active
			logger.info(remarkableMessage("make keeper active {}:{}"), backupKeeper.getIp(), backupKeeper.getPort());
			setKeeperState(backupKeeper, KeeperState.ACTIVE, redisMaster.getIp(), redisMaster.getPort());
			//make active backup
			logger.info(remarkableMessage("make keeper backup {}:{}"), activeKeeper.getIp(), activeKeeper.getPort());
			setKeeperState(activeKeeper, KeeperState.BACKUP, backupKeeper.getIp(), backupKeeper.getPort());
			
			sleep(2000);
			Assert.assertEquals(KeeperState.ACTIVE, getKeeperState(backupKeeper));
			Assert.assertEquals(KeeperState.BACKUP, getKeeperState(activeKeeper));

			KeeperMeta tmp = backupKeeper;
			backupKeeper = activeKeeper;
			activeKeeper = tmp;
		}
				

	}
	
	
	@Test
	public void testReFullSync() throws ExecuteException, IOException{
		
		RedisKeeperServer redisKeeperServer = getRedisKeeperServerActive(dc);
		DefaultReplicationStore replicationStore = (DefaultReplicationStore) redisKeeperServer.getReplicationStore();

		DcMeta dcMeta = getDcMeta();

		RedisMeta slave1 = createSlave(activeKeeper.getIp(), activeKeeper.getPort());

		int lastRdbUpdateCount = replicationStore.getRdbUpdateCount();
		logger.info(remarkableMessage("[testReFullSync][sendRandomMessage]"));
		sendRandomMessage(redisMaster, 1, replicationStoreCommandFileSize);
		
		for(int i=0; i < 3 ; i++){
			
			logger.info(remarkableMessage("[testReFullSync]{}"), i);
			startRedis(slave1);
			sleep(3000);
			int currentRdbUpdateCount = replicationStore.getRdbUpdateCount();
			logger.info("[testReFullSync]{},{}", lastRdbUpdateCount, currentRdbUpdateCount);
			Assert.assertEquals(lastRdbUpdateCount + 1, currentRdbUpdateCount);
			lastRdbUpdateCount = currentRdbUpdateCount;
			sendMesssageToMasterAndTest(100, getRedisMaster(), Lists.newArrayList(slave1));
		}
		
	}

	private RedisMeta createSlave(String masterIp, Integer masterPort) {
		
		RedisMeta slave = new RedisMeta();
		slave.setMaster(String.format("%s:%d", masterIp, masterPort));
		slave.setPort(randomPort());		
		return slave;
	}
}

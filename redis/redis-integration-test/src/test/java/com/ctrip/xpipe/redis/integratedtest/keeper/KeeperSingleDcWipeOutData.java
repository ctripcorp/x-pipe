package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import org.junit.Assert;
import org.junit.Test;


/**
 * @author wenchao.meng
 *
 * Dec 1, 2016
 */
public class KeeperSingleDcWipeOutData extends AbstractKeeperIntegratedSingleDc{
	
	@Test
	public void testWipeOutSlaveDataAndRestart() throws Exception{
		
		sendMessageToMasterAndTestSlaveRedis(10000);
		
		RedisKeeperServer slave = getRedisKeeperServer(backupKeeper);
		
		//clean slave
		slave.stop();
		slave.dispose();
		slave.destroy();
		
		remove(slave);
		//
		startKeeper(backupKeeper);
		setKeeperState(backupKeeper, KeeperState.BACKUP, activeKeeper.getIp(), activeKeeper.getPort());		
		RedisKeeperServer newSlave = getRedisKeeperServer(backupKeeper);
		
		//wait for slave to synchronize with master
		sleep(3000);
		Assert.assertEquals(PARTIAL_STATE.PARTIAL, newSlave.getRedisMaster().partialState());
		
		
		setKeeperState(backupKeeper, KeeperState.ACTIVE, redisMaster.getIp(), redisMaster.getPort());
		sleep(3000);
		//should be partial
		Assert.assertEquals(PARTIAL_STATE.PARTIAL, newSlave.getRedisMaster().partialState());
		
	}

}

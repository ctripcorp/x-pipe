package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.meta.server.job.SlaveofJob;
import com.ctrip.xpipe.redis.meta.server.job.XSlaveofJob;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

/**
 * @author wenchao.meng
 *
 *         Aug 17, 2016
 */
public class KeeperSingleDcSlaveof extends AbstractKeeperIntegratedSingleDc {


	@Test
	public void testXSlaveof() throws Exception {

		testMakeRedisSlave(true);
	}
	
	@Test
	public void testSlaveof() throws Exception {
		testMakeRedisSlave(false);
	}
	
	@Test
	public void testSlaveofBackup() throws Exception{
		
		sendMessageToMasterAndTestSlaveRedis();

		//test backupKeeper partial sync
		RedisKeeperServer backupKeeperServer = getRedisKeeperServer(backupKeeper);
		Set<RedisSlave> currentSlaves = backupKeeperServer.slaves();
		Assert.assertEquals(0, currentSlaves.size());

		logger.info(remarkableMessage("make slave slaves slaveof backup keeper"));
		new XSlaveofJob(slaves, backupKeeper.getIp(), backupKeeper.getPort(), getXpipeNettyClientKeyedObjectPool(), scheduled, executors).execute();
		
		logger.info(remarkableMessage("make backup keeper active"));
		//make backup active
		setKeeperState(backupKeeper, KeeperState.ACTIVE, redisMaster.getIp(), redisMaster.getPort());
		
		sleep(1000);
		
		currentSlaves = backupKeeperServer.slaves();
		Assert.assertEquals(slaves.size(), currentSlaves.size());
		for(RedisSlave redisSlave : currentSlaves){
			Assert.assertEquals(PARTIAL_STATE.PARTIAL, redisSlave.partialState());
		}

	}

	private void testMakeRedisSlave(boolean xslaveof) throws Exception {
		
		sendMessageToMasterAndTestSlaveRedis();

		RedisKeeperServer backupKeeperServer = getRedisKeeperServer(backupKeeper);

		setKeeperState(backupKeeper, KeeperState.ACTIVE, redisMaster.getIp(), redisMaster.getPort(), false);

		setKeeperState(activeKeeper, KeeperState.BACKUP, backupKeeper.getIp(), backupKeeper.getPort(), false);

		if (xslaveof) {
			new XSlaveofJob(slaves, backupKeeper.getIp(), backupKeeper.getPort(), getXpipeNettyClientKeyedObjectPool(), scheduled, executors).execute().sync();
		} else {
			new SlaveofJob(slaves, backupKeeper.getIp(), backupKeeper.getPort(), getXpipeNettyClientKeyedObjectPool(), scheduled, executors).execute().sync();
		}

		sleep(2000);
		Set<RedisSlave> slaves = backupKeeperServer.slaves();
		Assert.assertEquals(4, slaves.size());
		for (RedisSlave redisSlave : slaves) {

			PARTIAL_STATE dest = PARTIAL_STATE.PARTIAL;
			if (redisSlave.getSlaveListeningPort() == activeKeeper.getPort()) {
				logger.info("[testXSlaveof][role keeper]{}, {}", redisSlave, redisSlave.partialState());
			} else {
				logger.info("[testXSlaveof][role redis]{}, {}", redisSlave, redisSlave.partialState());
			}
			Assert.assertEquals(dest, redisSlave.partialState());
		}

		sendMessageToMasterAndTestSlaveRedis();
	}

}

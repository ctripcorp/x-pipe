package com.ctrip.xpipe.redis.integratedtest.keeper;

import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.meta.server.job.SlaveofJob;
import com.ctrip.xpipe.redis.meta.server.job.XSlaveofJob;

/**
 * @author wenchao.meng
 *
 *         Aug 17, 2016
 */
public class KeeperSingleDcSlaveof extends AbstractKeeperIntegratedSingleDc {

	private RedisMeta redisMaster;
	private KeeperMeta activeKeeper;
	private KeeperMeta backupKeeper;
	private List<RedisMeta> slaves;

	@Before
	public void beforeKeeperSingleDcRestart() {

		redisMaster = getRedisMaster();
		activeKeeper = getKeeperActive();
		backupKeeper = getKeepersBackup().get(0);
		slaves = getRedisSlaves();
	}

	@Test
	public void testXSlaveof() throws Exception {

		testMakeRedisSlave(true);

	}

	@Test
	public void testSlaveof() throws Exception {
		testMakeRedisSlave(false);
	}

	private void testMakeRedisSlave(boolean xslaveof) throws Exception {
		sendMessageToMasterAndTestSlaveRedis();

		RedisKeeperServer backupKeeperServer = getRedisKeeperServer(backupKeeper);

		setKeeperState(backupKeeper, KeeperState.ACTIVE, redisMaster.getIp(), redisMaster.getPort(), false);

		setKeeperState(activeKeeper, KeeperState.BACKUP, backupKeeper.getIp(), backupKeeper.getPort(), false);

		if (xslaveof) {
			new XSlaveofJob(slaves, backupKeeper.getIp(), backupKeeper.getPort(), clientPool).execute().sync();
		} else {
			new SlaveofJob(slaves, backupKeeper.getIp(), backupKeeper.getPort(), clientPool).execute().sync();
		}

		sleep(2000);
		Set<RedisSlave> slaves = backupKeeperServer.slaves();
		Assert.assertEquals(3, slaves.size());
		for (RedisSlave redisSlave : slaves) {

			PARTIAL_STATE dest = PARTIAL_STATE.PARTIAL;
			if (redisSlave.getSlaveListeningPort() == activeKeeper.getPort()) {
				logger.info("[testXSlaveof][role keeper]{}, {}", redisSlave, redisSlave.partialState());
			} else {
				logger.info("[testXSlaveof][role redis]{}, {}", redisSlave, redisSlave.partialState());
				if (!xslaveof) {
					dest = PARTIAL_STATE.FULL;
				}
			}
			Assert.assertEquals(dest, redisSlave.partialState());
		}

		sendMessageToMasterAndTestSlaveRedis();
	}

}

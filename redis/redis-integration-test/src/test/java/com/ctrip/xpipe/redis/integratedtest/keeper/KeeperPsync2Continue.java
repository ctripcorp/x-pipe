package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 *
 *         Feb 20, 2017
 */
public class KeeperPsync2Continue extends AbstractKeeperIntegratedSingleDc {

	private int testRound = 4;

	@Test
	public void testExchangeMasterSlave() throws Exception {

		RedisKeeperServer redisKeeperServer = getRedisKeeperServer(activeKeeper);
		int slaveChosen = 0;

		long fullSyncCount = redisKeeperServer.getKeeperMonitor().getKeeperStats().getFullSyncCount();

		for (int i = 0; i < testRound; i++) {

			logger.info(remarkableMessage("round:{}"), i);
			RedisMeta slaveToPromote = slaves.get(0);

			xslaveof(activeKeeper.getIp(), activeKeeper.getPort(), redisMaster);
			xslaveof(null, 0, slaveToPromote);
			setKeeperState(activeKeeper, KeeperState.ACTIVE, slaveToPromote.getIp(), slaveToPromote.getPort());

			sleep(3000);
			
			Assert.assertEquals(fullSyncCount,
					redisKeeperServer.getKeeperMonitor().getKeeperStats().getFullSyncCount());

			RedisMeta tmpRedisMaster = redisMaster;

			redisMaster = slaveToPromote;
			slaves.remove(slaveChosen);
			slaves.add(0, tmpRedisMaster);
		}

	}

}

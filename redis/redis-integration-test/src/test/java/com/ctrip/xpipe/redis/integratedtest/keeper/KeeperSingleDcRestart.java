package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 *
 *         Aug 17, 2016
 */
public class KeeperSingleDcRestart extends AbstractKeeperIntegratedSingleDc {

	@Test
	public void testBackupRestart() throws Exception {

		RedisKeeperServer redisKeeperServer = getRedisKeeperServer(backupKeeper);

		remove(redisKeeperServer);

		startKeeper(backupKeeper);
		RedisKeeperServer newRedisKeeperServer = getRedisKeeperServer(backupKeeper);
		Assert.assertNotEquals(newRedisKeeperServer, redisKeeperServer);

		Assert.assertEquals(KeeperState.PRE_BACKUP, newRedisKeeperServer.getRedisKeeperServerState().keeperState());

		setKeeperState(backupKeeper, KeeperState.BACKUP, activeKeeper.getIp(), activeKeeper.getPort());
		sleep(2000);

		Assert.assertEquals(KeeperState.BACKUP, newRedisKeeperServer.getRedisKeeperServerState().keeperState());
		Assert.assertEquals(PARTIAL_STATE.PARTIAL, newRedisKeeperServer.getRedisMaster().partialState());
	}

	@Test
	public void testActiveRestart() throws Exception {

		RedisKeeperServer redisKeeperServer = getRedisKeeperServer(activeKeeper);
		remove(redisKeeperServer);

		// make backup active
		logger.info(remarkableMessage("[make backup active]{}"), backupKeeper);
		RedisKeeperServer rawBackupServer = getRedisKeeperServer(backupKeeper);
		Assert.assertEquals(KeeperState.BACKUP, rawBackupServer.getRedisKeeperServerState().keeperState());
		setKeeperState(backupKeeper, KeeperState.ACTIVE, redisMaster.getIp(), redisMaster.getPort());
		sleep(2000);
		Assert.assertEquals(KeeperState.ACTIVE, rawBackupServer.getRedisKeeperServerState().keeperState());
		Assert.assertEquals(PARTIAL_STATE.PARTIAL, rawBackupServer.getRedisMaster().partialState());

		// start active again
		startKeeper(activeKeeper);
		RedisKeeperServer newRedisKeeperServer = getRedisKeeperServer(activeKeeper);
		Assert.assertNotEquals(newRedisKeeperServer, redisKeeperServer);

		Assert.assertEquals(KeeperState.PRE_ACTIVE, newRedisKeeperServer.getRedisKeeperServerState().keeperState());

		// make new keeper backup
		logger.info(remarkableMessage("[make old active backup]{}"), activeKeeper);
		setKeeperState(activeKeeper, KeeperState.BACKUP, backupKeeper.getIp(), backupKeeper.getPort());
		sleep(2000);

		Assert.assertEquals(KeeperState.BACKUP, newRedisKeeperServer.getRedisKeeperServerState().keeperState());
		Assert.assertEquals(PARTIAL_STATE.PARTIAL, newRedisKeeperServer.getRedisMaster().partialState());

	}

}

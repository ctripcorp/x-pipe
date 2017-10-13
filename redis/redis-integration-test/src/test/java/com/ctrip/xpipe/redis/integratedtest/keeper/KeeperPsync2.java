package com.ctrip.xpipe.redis.integratedtest.keeper;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;

/**
 * @author wenchao.meng
 *
 *         Feb 20, 2017
 */
public class KeeperPsync2 extends AbstractKeeperIntegratedSingleDc {

	private int totalKeepers = 5;

	private List<RedisKeeperServer> redisKeeperServers = new LinkedList<>();

	private int testRound = 5;

	@Test
	public void testKeeperPsync2() throws Exception {

		// init
		initKeepers();

		sleep(2000);

		assertSyncCount(redisKeeperServers);

		for (int i = 0; i < testRound; i++) {

			logger.info(remarkableMessage("[testRound]{}"), i);

			KeeperMeta lastKeeper = new KeeperMeta().setIp(redisMaster.getIp()).setPort(redisMaster.getPort());

			List<RedisKeeperServer> currentKeepers = new LinkedList<>();

			for (int j = 0; j < totalKeepers; j++) {

				int current = randomInt(0, redisKeeperServers.size() - 1);
				RedisKeeperServer currentRedisKeeperServer = redisKeeperServers.get(current);
				setKeeperState(currentRedisKeeperServer.getCurrentKeeperMeta(), KeeperState.ACTIVE, lastKeeper.getIp(),
						lastKeeper.getPort());

				redisKeeperServers.remove(currentRedisKeeperServer);

				logger.info("[testKeeperPsync2][slaveof]{}:{}  slaveof {}:{}",
						currentRedisKeeperServer.getCurrentKeeperMeta().getIp(),
						currentRedisKeeperServer.getCurrentKeeperMeta().getPort(), lastKeeper.getIp(),
						lastKeeper.getPort());

				lastKeeper = currentRedisKeeperServer.getCurrentKeeperMeta();
				currentKeepers.add(currentRedisKeeperServer);
			}
			sendMessageToMaster(redisMaster, 10);
			redisKeeperServers = currentKeepers;

			sleep(2000);
			assertSyncCount(redisKeeperServers);
			assertCommandsEquals(redisKeeperServers);
		}
	}

	private void assertCommandsEquals(List<RedisKeeperServer> redisKeeperServers) {

		long end = redisKeeperServers.get(0).getReplicationStore().getEndOffset();
		for (int i = 1; i < redisKeeperServers.size(); i++) {
			Assert.assertEquals(end, redisKeeperServers.get(i).getReplicationStore().getEndOffset());
		}

	}

	private void assertSyncCount(List<RedisKeeperServer> redisKeeperServers) {

		logger.info("[assertSyncCount]");

		int full = 0, partialError = 0;
		for (RedisKeeperServer redisKeeperServer : redisKeeperServers) {
			full += redisKeeperServer.getKeeperMonitor().getKeeperStats().getFullSyncCount();
			partialError += redisKeeperServer.getKeeperMonitor().getKeeperStats().getPartialSyncErrorCount();
		}
		Assert.assertEquals(0, partialError);
		Assert.assertEquals(totalKeepers - 1, full);
	}

	private void initKeepers() throws Exception {

		redisKeeperServers.add(getRedisKeeperServer(activeKeeper));
		redisKeeperServers.add(getRedisKeeperServer(backupKeeper));

		setKeeperState(backupKeeper, KeeperState.ACTIVE, activeKeeper.getIp(), activeKeeper.getPort());

		KeeperMeta lastKeeper = backupKeeper;
		List<KeeperMeta> keeperMetas = new LinkedList<>();

		int portStart = Math.max(activeKeeper.getPort(), backupKeeper.getPort());

		for (int i = 0; i < totalKeepers - redisKeeperServers.size(); i++) {
			KeeperMeta keeperMeta = new KeeperMeta().setIp("localhost").setPort(++portStart);
			keeperMeta.setParent(activeKeeper.parent());
			keeperMetas.add(keeperMeta);
		}

		for (KeeperMeta keeperMeta : keeperMetas) {

			RedisKeeperServer redisKeeperServer = startKeeper(keeperMeta);
			setKeeperState(keeperMeta, KeeperState.ACTIVE, lastKeeper.getIp(), lastKeeper.getPort());

			logger.info("[initKeepers][slaveof]{}:{}  slaveof {}:{}", keeperMeta.getIp(), keeperMeta.getPort(),
					lastKeeper.getIp(), lastKeeper.getPort());

			redisKeeperServers.add(redisKeeperServer);
			lastKeeper = keeperMeta;
		}
	}

	@Override
	protected void startRedis(RedisMeta redisMeta) throws IOException {

		if (redisMeta.equals(getRedisMaster())) {
			super.startRedis(redisMeta);
		} else {
			logger.info("[startRedis][do not start it]{}", redisMeta);
		}
	}

	@Override
	protected KeeperConfig getKeeperConfig() {
		return new TestKeeperConfig(1 << 20, 1 << 10, Long.MAX_VALUE, 60 * 1000);
	}

}

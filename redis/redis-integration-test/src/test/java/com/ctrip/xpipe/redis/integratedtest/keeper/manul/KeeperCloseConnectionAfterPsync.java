package com.ctrip.xpipe.redis.integratedtest.keeper.manul;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.integratedtest.keeper.AbstractKeeperIntegratedSingleDc;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.monitor.KeepersMonitorManager;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import io.netty.channel.Channel;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wenchao.meng
 *
 * Sep 29, 2016
 */
public class KeeperCloseConnectionAfterPsync extends AbstractKeeperIntegratedSingleDc{

	private AtomicInteger count = new AtomicInteger();
	
	@Test
	public void testRedis() throws Exception{

		waitForAnyKeyToExit();
	}

	@Override
	protected RedisKeeperServer createRedisKeeperServer(KeeperMeta keeperMeta, File baseDir, KeeperConfig keeperConfig,
														LeaderElectorManager leaderElectorManager, KeepersMonitorManager keeperMonitorManager,
														SyncRateManager syncRateManager) {

		return new DefaultRedisKeeperServer(keeperMeta.parent().getDbId(), keeperMeta, keeperConfig, baseDir, leaderElectorManager,
				keeperMonitorManager, resourceManager, syncRateManager){

			@Override
			protected void becomeSlave(Channel channel, RedisSlave redisSlave) {
				super.becomeSlave(channel, redisSlave);

				int current = count.incrementAndGet();

				logger.info("count: {}", current);
				if(current >= 2  && current <= 4){
					logger.info("close slave. {}", redisSlave);
					try {
						redisSlave.close();
					} catch (IOException e) {
						logger.error("error close slave" + redisSlave, e);
					}
				}

			}
		};
	}

	@Override
	protected String getXpipeMetaConfigFile() {
		return "one_keeper.xml";
	}

	@Override
	protected KeeperConfig getKeeperConfig() {
		return new TestKeeperConfig(1 << 30, 5, 1 << 30, 300000);
	}

}
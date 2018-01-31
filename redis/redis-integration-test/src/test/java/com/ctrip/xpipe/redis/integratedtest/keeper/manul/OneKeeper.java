package com.ctrip.xpipe.redis.integratedtest.keeper.manul;

import com.ctrip.xpipe.redis.integratedtest.keeper.AbstractKeeperIntegratedSingleDc;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import io.netty.util.ResourceLeakDetector;
import org.junit.Test;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * Sep 29, 2016
 */
public class OneKeeper extends AbstractKeeperIntegratedSingleDc{

	@Override
	public void beforeAbstractTest() throws Exception {
		super.beforeAbstractTest();
		ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.SIMPLE);

	}

	@Test
	public void statePreActive() throws IOException{
		
		try{
			sendMessageToMaster(redisMaster, 10);
			
			RedisKeeperServer redisKeeperServer = getRedisKeeperServer(activeKeeper);

			
			logger.info(remarkableMessage("[statePreActive][stop dispose]"));
			redisKeeperServer.stop();
			redisKeeperServer.dispose();
			
			sleep(100);
			logger.info(remarkableMessage("[statePreActive][initialize start]"));
			
			redisKeeperServer.initialize();
			redisKeeperServer.start();
		}catch(Throwable e){
			logger.error("[startTest]", e);
		}
		
		waitForAnyKeyToExit();
	}
	
	@Test
	public void testRedis() throws Exception{
		
		waitForAnyKeyToExit();
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
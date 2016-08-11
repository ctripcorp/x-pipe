/**
 * 
 */
package com.ctrip.xpipe.redis.keeper.impl;

import java.io.IOException;
import java.util.Map;

import org.junit.After;
import org.junit.Test;

import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;

/**
 * @author marsqing
 *
 *         Jun 16, 2016 5:27:17 PM
 */
public class JqRedisKeeperServerTest extends BaseRedisKeeperServerTest {
	
	@Override
	protected boolean deleteTestDir() {
		return false;
	}

	@Test
	public void startKeeper() throws Exception {

		startKeeper("keeper-start.xml", "jq");
	}

	@Test
	public void testStartStop() throws Exception{
		
		startKeeper("keeper-start.xml", "jq");
		
		waitForAnyKeyToExit();
		
		Map<String, RedisKeeperServer> servers = getRegistry().getComponents(RedisKeeperServer.class);
		RedisKeeperServer redisKeeperServer = (RedisKeeperServer) servers.values().toArray()[0];

		logger.info("start stop test");
		for(int i=0;i<3;i++){
			
			sleep(2000);
			logger.info("[stop]{}", i);
			redisKeeperServer.stop();
			sleep(2000);
			logger.info("[start]{}", i);
			redisKeeperServer.start();
		}
		
		logger.info("dispose test");
		redisKeeperServer.stop();
		redisKeeperServer.dispose();
		
		sleep(2000);
		logger.info("init start test");
		redisKeeperServer.initialize();
		redisKeeperServer.start();
		
		waitForAnyKeyToExit();
	}

	@After
	public void afterOneBoxTest() throws IOException {

	}

}

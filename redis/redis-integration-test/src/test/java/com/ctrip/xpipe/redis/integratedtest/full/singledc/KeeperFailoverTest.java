package com.ctrip.xpipe.redis.integratedtest.full.singledc;

import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * Jun 13, 2016
 */
public class KeeperFailoverTest extends AbstractSingleDcTest{
	
	
	@Test
	public void testKeeperFailOver() throws Exception{
		
		RedisKeeperServer activeRedisKeeperServer = getRedisKeeperServerActive();
		logger.info("[testKeeperFailOver][active]{}", activeRedisKeeperServer);
		sleep(6000);
		
		logger.info(remarkableMessage("[testKeeperFailOver][stop activeKeeper]"), activeRedisKeeperServer);
		remove(activeRedisKeeperServer);
		sleep(6000);
		
		RedisKeeperServer activeNow = getRedisKeeperServerActive();
		logger.info("[testKeeperFailOver][active new]{}", activeNow);
		Assert.assertNotNull(activeNow);
		Assert.assertNotEquals(activeRedisKeeperServer, activeNow);
		
		Assert.assertEquals(PARTIAL_STATE.PARTIAL, activeNow.getRedisMaster().partialState());
		for(RedisSlave slave : activeNow.slaves()){
			Assert.assertEquals(PARTIAL_STATE.PARTIAL, slave.partialState());
		}
		
		sendMessageToMasterAndTestSlaveRedis();
		
	}


	@After
	public void afterKeeperFailOverTest() throws IOException{
	}

}

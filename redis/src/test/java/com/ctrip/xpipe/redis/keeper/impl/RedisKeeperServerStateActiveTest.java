package com.ctrip.xpipe.redis.keeper.impl;



import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.redis.keeper.entity.KeeperMeta;



/**
 * @author wenchao.meng
 *
 * Jun 12, 2016
 */
public class RedisKeeperServerStateActiveTest extends AbstractRedisKeeperServerStateTest{
	
	private RedisKeeperServerStateActive active;
	
	@Before
	public void beforeRedisKeeperServerStateTest() throws Exception{

		active = new RedisKeeperServerStateActive(redisKeeperServer, 
				redisKeeperServer.getCurrentKeeperMeta(), redisMasterMeta);
	}

	@Test
	public void getMaster(){
		
		Assert.assertEquals(redisMasterMeta.getIp(), active.getMaster().getHost());
		Assert.assertEquals(redisMasterMeta.getPort(), (Integer)active.getMaster().getPort());
	}
	
	@Test
	public void testActiveActive(){
		
		update(redisKeeperServer.getCurrentKeeperMeta(), active);
		
	}

	@Test(expected = NullPointerException.class)
	public void testActiveBackup(){
		
		
		KeeperMeta keeperMeta = createKeeperMeta();
		keeperMeta.setPort(redisKeeperServer.getCurrentKeeperMeta().getPort() + 1);
		
		update(keeperMeta, active);
	}

	

	@After
	public void afterRedisKeeperServerStateTest(){
		
	}
}

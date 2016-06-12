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
public class RedisKeeperServerStateBackupTest extends AbstractRedisKeeperServerStateTest{
	
	private RedisKeeperServerStateBackup backup;
	private KeeperMeta keeperMeta;
	
	@Before
	public void beforeRedisKeeperServerStateTest() throws Exception{

		keeperMeta = createKeeperMeta();
		keeperMeta.setPort(redisKeeperServer.getCurrentKeeperMeta().getPort() + 1);
		
		backup = new RedisKeeperServerStateBackup(redisKeeperServer, 
				keeperMeta, redisMasterMeta);
	}

	@Test
	public void getMaster(){
		
		Assert.assertEquals(keeperMeta.getIp(), backup.getMaster().getHost());
		Assert.assertEquals(keeperMeta.getPort(), (Integer)backup.getMaster().getPort());
	}
	
	@Test(expected = IllegalStateException.class)
	public void testBackupActive(){
		
		update(redisKeeperServer.getCurrentKeeperMeta(), backup);
		
	}

	@Test
	public void testBackupBackup(){
		
		
		KeeperMeta newKeeperMeta = createKeeperMeta();
		newKeeperMeta.setPort(keeperMeta.getPort() + 1);
		update(newKeeperMeta, backup);
		
	}

	

	@After
	public void afterRedisKeeperServerStateTest(){
		
	}
}

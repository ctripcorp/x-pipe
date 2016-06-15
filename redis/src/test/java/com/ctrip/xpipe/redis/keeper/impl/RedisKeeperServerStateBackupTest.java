package com.ctrip.xpipe.redis.keeper.impl;



import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.redis.keeper.entity.KeeperMeta;
import com.ctrip.xpipe.redis.keeper.meta.ShardStatus;


/**
 * @author wenchao.meng
 *
 * Jun 12, 2016
 */
public class RedisKeeperServerStateBackupTest extends AbstractRedisKeeperServerStateTest{
	
	private RedisKeeperServerStateBackup backup;
	private KeeperMeta activeKeeperMeta;
	
	@Before
	public void beforeRedisKeeperServerStateTest() throws Exception{

		activeKeeperMeta = createKeeperMeta();
		activeKeeperMeta.setPort(redisKeeperServer.getCurrentKeeperMeta().getPort() + 1);
		
		ShardStatus shardStatus = createShardStatus(activeKeeperMeta, null, redisMasterMeta);
		backup = new RedisKeeperServerStateBackup(redisKeeperServer, shardStatus);
	}

	@Test
	public void getMaster(){
		
		Assert.assertEquals(activeKeeperMeta.getIp(), backup.getMaster().getHost());
		Assert.assertEquals(activeKeeperMeta.getPort(), (Integer)backup.getMaster().getPort());
	}
	
	@Test
	public void testBackupActive(){

		ShardStatus shardStatus = createShardStatus(redisKeeperServer.getCurrentKeeperMeta(), null, backup.getShardStatus().getRedisMaster());
		try{
			update(shardStatus, backup);
			Assert.fail();
		}catch(Exception e){
			e.printStackTrace();
		}
				
	}

	@Test
	public void testBackupBackup(){
		
		KeeperMeta newKeeperMeta = createKeeperMeta();
		newKeeperMeta.setPort(activeKeeperMeta.getPort() + 1);
		
		ShardStatus shardStatus = createShardStatus(newKeeperMeta, null, backup.getShardStatus().getRedisMaster());
		update(shardStatus, backup);
	}

	@After
	public void afterRedisKeeperServerStateTest(){
		
	}
}

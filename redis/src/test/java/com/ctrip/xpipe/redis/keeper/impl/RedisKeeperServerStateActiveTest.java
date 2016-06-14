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
public class RedisKeeperServerStateActiveTest extends AbstractRedisKeeperServerStateTest{
	
	private RedisKeeperServerStateActive active;
	
	@Before
	public void beforeRedisKeeperServerStateTest() throws Exception{
		
		ShardStatus shardStatus = createShardStatus(redisKeeperServer.getCurrentKeeperMeta(), null, redisMasterMeta);
		active = new RedisKeeperServerStateActive(redisKeeperServer,  shardStatus);
	}


	@Test
	public void getMaster(){
		
		Assert.assertEquals(redisMasterMeta.getIp(), active.getMaster().getHost());
		Assert.assertEquals(redisMasterMeta.getPort(), (Integer)active.getMaster().getPort());
		
		KeeperMeta upstreamKeeper = createKeeperMeta();
		upstreamKeeper.setPort(redisMasterMeta.getPort() + 1);
		ShardStatus newStatus = createShardStatus(redisKeeperServer.getCurrentKeeperMeta(), upstreamKeeper, null);
		
		update(newStatus, active);

		Assert.assertEquals(upstreamKeeper.getIp(), active.getMaster().getHost());
		Assert.assertEquals(upstreamKeeper.getPort(), (Integer)active.getMaster().getPort());
}
	
	@Test
	public void testActiveActive(){
		
		update(active.getShardStatus(), active);
		
	}

	@Test
	public void testActiveBackup(){
		
		
		KeeperMeta keeperMeta = createKeeperMeta();
		keeperMeta.setPort(redisKeeperServer.getCurrentKeeperMeta().getPort() + 1);
		
		ShardStatus newStatus = createShardStatus(keeperMeta, active.getShardStatus().getUpstreamKeeper(), active.getShardStatus().getRedisMaster());
		
		update(newStatus, active);
		
		Assert.assertTrue(redisKeeperServer.getRedisKeeperServerState() instanceof RedisKeeperServerStateBackup);
	}

	

	@After
	public void afterRedisKeeperServerStateTest(){
		
	}
}

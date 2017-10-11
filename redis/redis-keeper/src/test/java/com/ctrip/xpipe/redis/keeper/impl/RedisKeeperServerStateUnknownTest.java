package com.ctrip.xpipe.redis.keeper.impl;


import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.ShardStatus;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServerState;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;




/**
 * @author wenchao.meng
 *
 * Jun 12, 2016
 */
public class RedisKeeperServerStateUnknownTest extends AbstractRedisKeeperServerStateTest{
	
	private RedisKeeperServerStateUnknown unknown;
	
	@Before
	public void beforeRedisKeeperServerStateTest() throws Exception{

		unknown = new RedisKeeperServerStateUnknown(redisKeeperServer);
	}

	@Test
	public void testActive() throws IOException{
				
		//active
		KeeperMeta keeperMeta = redisKeeperServer.getCurrentKeeperMeta();
		ShardStatus shardStatus = createShardStatus(keeperMeta, null, redisMasterMeta);
		unknown.setShardStatus(shardStatus);
		
		
		RedisKeeperServerState newState = redisKeeperServer.getRedisKeeperServerState();
		
		Assert.assertTrue(newState instanceof RedisKeeperServerStateActive);
		Assert.assertEquals(new InetSocketAddress(redisMasterMeta.getIp(), redisMasterMeta.getPort()),
				newState.getMaster().getSocketAddress());
	}

	@Test
	public void testBackup() throws IOException{
				
		//active
		KeeperMeta keeperMeta = SerializationUtils.clone(redisKeeperServer.getCurrentKeeperMeta());
		keeperMeta.setPort(keeperMeta.getPort() + 1);
		ShardStatus shardStatus = createShardStatus(keeperMeta, null, redisMasterMeta);
		unknown.setShardStatus(shardStatus);
		
		RedisKeeperServerState newState = redisKeeperServer.getRedisKeeperServerState();
		Assert.assertTrue(newState instanceof RedisKeeperServerStateBackup);
		Assert.assertEquals(new InetSocketAddress(keeperMeta.getIp(), keeperMeta.getPort()),
				newState.getMaster().getSocketAddress());
		
	}


	@After
	public void afterRedisKeeperServerStateTest(){
		
	}
}

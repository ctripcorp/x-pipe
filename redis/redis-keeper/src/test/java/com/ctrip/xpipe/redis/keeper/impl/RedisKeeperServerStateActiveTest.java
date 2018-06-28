package com.ctrip.xpipe.redis.keeper.impl;


import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.ShardStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.InetSocketAddress;

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
		active = new RedisKeeperServerStateActive(redisKeeperServer);
		active.setShardStatus(shardStatus);
	}


	@Test
	public void getMaster() throws IOException, SAXException{
		
		Assert.assertEquals(new InetSocketAddress(redisMasterMeta.getIp(), redisMasterMeta.getPort()), active.getMaster().getSocketAddress());
		
		KeeperMeta upstreamKeeper = createKeeperMeta();
		upstreamKeeper.setPort(redisMasterMeta.getPort() + 1);
		ShardStatus newStatus = createShardStatus(redisKeeperServer.getCurrentKeeperMeta(), upstreamKeeper, null);

		active.setShardStatus(newStatus);

		Assert.assertEquals(new InetSocketAddress(upstreamKeeper.getIp(), upstreamKeeper.getPort()), active.getMaster().getSocketAddress());
}
	
	@Test
	public void testActiveActive(){
		
		active.becomeActive(new DefaultEndPoint("localhost", randomPort()));
		
	}

	@Test
	public void testActiveBackup() throws IOException{

		active.becomeBackup(new DefaultEndPoint("localhost", randomPort()));
		Assert.assertTrue(redisKeeperServer.getRedisKeeperServerState() instanceof RedisKeeperServerStateBackup);
		
	}

	@After
	public void afterRedisKeeperServerStateTest(){
		
	}
}

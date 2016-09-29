package com.ctrip.xpipe.redis.keeper.impl;

import java.net.InetSocketAddress;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.ShardStatus;

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
		backup = new RedisKeeperServerStateBackup(redisKeeperServer);
		backup.setShardStatus(shardStatus);
	}

	@Test
	public void getMaster(){
		
		Assert.assertEquals(new InetSocketAddress(activeKeeperMeta.getIp(), activeKeeperMeta.getPort()), backup.getMaster().getSocketAddress());
	}
	
	@Test
	public void testBackupActive(){

		backup.becomeActive(new InetSocketAddress("localhost", randomPort()));
	}

	@Test
	public void testBackupBackup(){
		
		backup.becomeBackup(new InetSocketAddress("localhost", randomPort()));
	}

	@After
	public void afterRedisKeeperServerStateTest(){
		
	}
}

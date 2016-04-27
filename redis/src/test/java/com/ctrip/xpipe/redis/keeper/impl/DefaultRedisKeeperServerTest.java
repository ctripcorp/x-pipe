package com.ctrip.xpipe.redis.keeper.impl;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.AbstractRedisTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.ReplicationStore;

/**
 * @author wenchao.meng
 *
 * 2016年4月21日 下午5:42:29
 */
public class DefaultRedisKeeperServerTest extends AbstractRedisTest{
	
	private RedisKeeperServer redisKeeperServer;
	private String keeperRunid = randomString(runidLength);
	private Endpoint masterEndpoint = new DefaultEndPoint("redis://127.0.0.1:6379"); 
	private ReplicationStore replicationStore;
	private int keeperPort = 7777;
	
	@Before
	public void beforeDefaultRedisKeeperServerTest() throws IOException{
		replicationStore = createReplicationStore();
		
	}
	
	@Test
	public void simpleTest() throws Exception{
		
		redisKeeperServer = new DefaultRedisKeeperServer(masterEndpoint, replicationStore, keeperRunid, keeperPort);
		redisKeeperServer.start();
	}

	
	@After
	public void afterDefaultRedisKeeperServerTest(){
		sleepSeconds(1000);
	}
}

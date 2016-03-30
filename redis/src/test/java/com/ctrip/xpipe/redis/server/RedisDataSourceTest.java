package com.ctrip.xpipe.redis.server;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.UnknownHostException;

import org.junit.After;
import org.junit.Test;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.payload.FileInOutPayload;
import com.ctrip.xpipe.redis.AbstractRedisTest;
import com.ctrip.xpipe.redis.server.impl.DefaultRedisSlaveServer;

/**
 * @author wenchao.meng
 *
 * 2016年3月30日 上午9:00:35
 */
public class RedisDataSourceTest extends AbstractRedisTest{
	
	
	private String rdbFile = "/data/xpipe/redis/dump.rdb";
	private String commandFile = "/data/xpipe/redis/command.out";
	private String redisUri = "redis://10.2.58.242:6379";
	
	@Test
	public void test(){
		
	}
	
	@Test
	public void testFull() throws InterruptedException, UnknownHostException, IOException{
		
		InOutPayload rdbPayload = new FileInOutPayload(rdbFile);
		FileOutputStream command = new FileOutputStream(new File(commandFile));
		DefaultRedisSlaveServer rds = new DefaultRedisSlaveServer(new DefaultEndPoint(redisUri), rdbPayload, command);
		executors.execute(rds);
	}

	
	@After
	public void afterRedisDataSourceTest(){

		sleepSeconds(1000);
	}
}

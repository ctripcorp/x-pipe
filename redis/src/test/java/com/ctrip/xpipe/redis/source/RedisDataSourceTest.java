package com.ctrip.xpipe.redis.source;


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

public class RedisDataSourceTest extends AbstractRedisTest{
	
	@Test
	public void test(){
		
	}
	
	@Test
	public void testFull() throws InterruptedException, UnknownHostException, IOException{
		
		InOutPayload rdbPayload = new FileInOutPayload("/data/xpipe/redis/dump.rdb");
		FileOutputStream command = new FileOutputStream(new File("/data/xpipe/redis/command.out"));
		DefaultRedisSlaveServer rds = new DefaultRedisSlaveServer(new DefaultEndPoint("redis://10.2.58.242:6379"), rdbPayload, command);
		executors.execute(rds);
		
	}

	
	@After
	public void afterRedisDataSourceTest(){

		sleepSeconds(1000);
	}
}

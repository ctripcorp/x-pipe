package com.ctrip.xpipe.redis.source;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.UnknownHostException;

import org.junit.After;
import org.junit.Test;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.AbstractRedisTest;
import com.ctrip.xpipe.redis.rdb.RdbWriter;
import com.ctrip.xpipe.redis.rdb.impl.RdbFileWriter;
import com.ctrip.xpipe.redis.server.RedisSlaveServer;

public class RedisDataSourceTest extends AbstractRedisTest{
	
	@Test
	public void test(){
		
	}
	
	@Test
	public void testFull() throws InterruptedException, UnknownHostException, IOException{
		
		RdbWriter rdbWriter = new RdbFileWriter("/data/xpipe/redis/dump.rdb");
		FileOutputStream command = new FileOutputStream(new File("/data/xpipe/redis/command.out"));
		RedisSlaveServer rds = new RedisSlaveServer(new DefaultEndPoint("redis://10.2.58.242:6379"), rdbWriter, command);
		executors.execute(rds);
		
	}

	
	@After
	public void afterRedisDataSourceTest(){

		sleepSeconds(1000);
	}
}

package com.ctrip.xpipe.redis.integratedtest;

import java.io.IOException;

import org.junit.After;
import org.junit.Test;

/**
 * 
 * redis_master -> keeper -> redis_slave
 * @author wenchao.meng
 *
 * Jun 13, 2016
 */
public class DataSyncTest extends AbstractIntegratedTest{
	
	
	@Test
	public void simpleTest() throws IOException{
		
		
		
	}
	
	@After
	public void afterStartMetaServer() throws IOException{
		System.out.println("Press any key to exit...");
		System.in.read();
	}
}

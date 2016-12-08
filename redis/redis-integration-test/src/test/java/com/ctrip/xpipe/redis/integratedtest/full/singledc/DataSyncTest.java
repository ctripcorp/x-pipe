package com.ctrip.xpipe.redis.integratedtest.full.singledc;

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
public class DataSyncTest extends AbstractSingleDcTest{
	
	
	@Test
	public void simpleTest() throws Exception{
		try{
			sendMessageToMasterAndTestSlaveRedis();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	
	@After
	public void afterStartMetaServer() throws IOException{
	}
}

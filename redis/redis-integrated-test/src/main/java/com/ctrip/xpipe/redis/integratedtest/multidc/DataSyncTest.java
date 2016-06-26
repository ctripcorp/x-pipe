package com.ctrip.xpipe.redis.integratedtest.multidc;

import java.io.IOException;

import org.junit.Test;

/**
 * @author wenchao.meng
 *
 * Jun 21, 2016
 */
public class DataSyncTest extends AbstractMultiDcTest{
	
	
	@Test
	public void testSync() throws IOException{
		
		sendMessageToMasterAndTestSlaveRedis();
		
		waitForAnyKeyToExit();
	}
	
	

}

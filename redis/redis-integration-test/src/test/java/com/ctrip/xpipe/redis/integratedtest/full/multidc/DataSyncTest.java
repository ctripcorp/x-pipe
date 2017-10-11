package com.ctrip.xpipe.redis.integratedtest.full.multidc;

import org.junit.Test;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * Jun 21, 2016
 */
public class DataSyncTest extends AbstractMultiDcTest{
	
	
	@Test
	public void testSync() throws IOException{
		
		sendMessageToMasterAndTestSlaveRedis();
		
	}
	
}

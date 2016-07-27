package com.ctrip.xpipe.redis.integratedtest.multidcv1;

import java.io.IOException;

import org.junit.Test;

import com.ctrip.xpipe.redis.integratedtest.multidc.AbstractMultiDcTest;

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

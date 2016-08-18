package com.ctrip.xpipe.redis.integratedtest.keeper;


import java.io.IOException;

import org.junit.Test;


/**
 * @author wenchao.meng
 *
 * Aug 17, 2016
 */
public class KeeperMultiDc extends AbstractKeeperIntegratedMultiDc{
	
	@Test
	public void testSync() throws IOException{
		
		sendMessageToMasterAndTestSlaveRedis();
	}
	


}

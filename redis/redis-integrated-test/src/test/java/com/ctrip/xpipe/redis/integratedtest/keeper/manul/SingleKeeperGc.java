package com.ctrip.xpipe.redis.integratedtest.keeper.manul;


import java.io.IOException;

import org.junit.Test;

import com.ctrip.xpipe.redis.integratedtest.keeper.AbstractKeeperIntegratedSingleDc;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;

/**
 * @author wenchao.meng
 *
 * Sep 29, 2016
 */
public class SingleKeeperGc extends AbstractKeeperIntegratedSingleDc{
	
	@Test
	public void startTest() throws IOException{
		
		sendMessageToMasterAndTestSlaveRedis();
		
		waitForAnyKeyToExit();
	}
	
	@Override
	protected KeeperConfig getKeeperConfig() {
		return new TestKeeperConfig(1 << 30, 5, 1 << 30, 300000);
	}

}

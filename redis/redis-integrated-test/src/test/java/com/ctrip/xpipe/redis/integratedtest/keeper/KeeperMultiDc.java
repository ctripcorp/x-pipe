package com.ctrip.xpipe.redis.integratedtest.keeper;


import java.io.IOException;

import org.junit.Test;

import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;


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

	@Override
	protected KeeperConfig getKeeperConfig() {
		return new TestKeeperConfig(1 << 20, 100, 100 * (1 << 20), 2000);
	}


}

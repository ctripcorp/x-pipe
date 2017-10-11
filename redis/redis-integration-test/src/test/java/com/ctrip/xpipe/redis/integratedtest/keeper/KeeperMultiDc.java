package com.ctrip.xpipe.redis.integratedtest.keeper;


import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import org.junit.Test;

import java.io.IOException;


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

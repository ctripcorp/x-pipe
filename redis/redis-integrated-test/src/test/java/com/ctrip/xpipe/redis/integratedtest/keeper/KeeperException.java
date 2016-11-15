package com.ctrip.xpipe.redis.integratedtest.keeper;

import org.junit.Test;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;


/**
 * @author wenchao.meng
 *
 * Nov 15, 2016
 */
public class KeeperException extends AbstractKeeperIntegratedSingleDc{
	
	@Test
	public void testConnectToSlaveWhileSlaveNotConnectedWithItsMaster() throws Exception{

		logger.info(remarkableMessage("make active keeper connect to redis slave"));
		
		waitForAnyKey();
		
		KeeperMeta activeKeeper = getKeeperActive();
		RedisMeta slave = getRedisSlaves().get(0);
		setKeeperState(activeKeeper, KeeperState.ACTIVE, slave.getIp(), slave.getPort());
		
		
		waitForAnyKeyToExit();
	}

}

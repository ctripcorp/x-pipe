package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import org.junit.Test;

/**
 * @author wenchao.meng
 *
 * Aug 17, 2016
 */
public class KeeperSingleDcEof extends AbstractKeeperIntegratedSingleDc{
	
	@Test
	public void testEofRestart() throws Exception{

		sendMessageToMasterAndTestSlaveRedis();
		
		logger.info(remarkableMessage("stop keepers"));

		RedisKeeperServer active = getRedisKeeperServer(activeKeeper);
		LifecycleHelper.stopIfPossible(active);
		LifecycleHelper.disposeIfPossible(active);
	
		RedisKeeperServer backup = getRedisKeeperServer(backupKeeper);
		LifecycleHelper.stopIfPossible(backup);
		LifecycleHelper.disposeIfPossible(backup);

		logger.info(remarkableMessage("start keepers"));

		startKeepers();
		makeKeeperRight();
		
		sleep(2000);

		sendMessageToMasterAndTestSlaveRedis();
	}

	
	public boolean isEof() {
		return true;
	}
}

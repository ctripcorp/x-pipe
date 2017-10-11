package com.ctrip.xpipe.redis.integratedtest.keeper.manul;

import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.monitor.CatConfig;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.integratedtest.keeper.AbstractKeeperIntegratedSingleDc;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * Sep 29, 2016
 */
public class OneDcKeepers extends AbstractKeeperIntegratedSingleDc{

	@Override
	protected void doBeforeAbstractTest() throws Exception {
		System.setProperty(CatConfig.CAT_ENABLED_KEY, "true");
	}
	
	@Test
	public void startTest() throws IOException{
		
		try{
			sendMessageToMasterAndTestSlaveRedis();
		}catch(Throwable e){
			logger.error("[startTest]", e);
		}
		
		waitForAnyKeyToExit();
	}

	@Override
	protected void doAfterAbstractTest() throws Exception {
		super.doAfterAbstractTest();
	}
	
	@Test
	public void killActive() throws Exception{
		
		RedisMeta redisMaster = getRedisMaster();

		sendMessageToMasterAndTestSlaveRedis();

		System.out.println("press any key to make back keeper active");		
		waitForAnyKey();
		
		KeeperMeta backupKeeper = getKeepersBackup().get(0);
		RedisKeeperServer redisKeeperServer = getRedisKeeperServer(backupKeeper);
		Assert.assertEquals(PARTIAL_STATE.FULL, redisKeeperServer.getRedisMaster().partialState());
		logger.info(remarkableMessage("make keeper active{}"), backupKeeper);
		setKeeperState(backupKeeper, KeeperState.ACTIVE, redisMaster.getIp(), redisMaster.getPort());
		
		
		waitForAnyKeyToExit();
}

	@Override
	protected KeeperConfig getKeeperConfig() {
		return new TestKeeperConfig(1 << 30, 5, 1 << 30, 300000);
	}

}
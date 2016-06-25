package com.ctrip.xpipe.redis.integratedtest;

import java.io.IOException;

import org.apache.commons.exec.ExecuteException;
import org.junit.Assert;

import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.exception.RedisSlavePromotionException;
import com.ctrip.xpipe.redis.meta.server.impl.DefaultMetaServer;

/**
 * @author wenchao.meng
 *
 * Jun 22, 2016
 */
public abstract class AbstractIntegratedTestTemplate extends AbstractIntegratedTest{
	
	
	protected void sendMessageToMasterAndTestSlaveRedis() {
		
		sendRandomMessage(getRedisMaster(), getTestMessageCount());
		sleep(6000);
		assertRedisEquals(getRedisMaster(), getRedisSlaves());
	}


	
	protected void failOverTestTemplate() throws ExecuteException, IOException, RedisSlavePromotionException {
		
		RedisMeta redisMaster = getRedisMaster();
		stopServerListeningPort(redisMaster.getPort());
		
		RedisKeeperServer redisKeeperServer = getRedisKeeperServerActive(activeDc().getId());
		
		RedisMeta slave = getRedisSlaves(activeDc().getId()).get(0);

		SERVER_ROLE role  = getRedisServerRole(slave);
		Assert.assertEquals(SERVER_ROLE.SLAVE, role);

		logger.info("[testRedisFailover][promote]{}:{}", slave.getIp(), slave.getPort());
		//TODO console ready delete this
		DefaultMetaServer metaServer = getDcInfos().get(activeDc().getId()).getApplicationContext().getBean(DefaultMetaServer.class);
		metaServer.updateRedisMaster(slave.parent().parent().getId(), slave.parent().getId(), slave);
		redisKeeperServer.promoteSlave(slave.getIp(), slave.getPort());
		changeRedisMaster(redisMaster, slave);
		
		sleep(6000);
		role  = getRedisServerRole(slave);
		Assert.assertEquals(SERVER_ROLE.MASTER, role);
		
		Assert.assertEquals(PARTIAL_STATE.PARTIAL, redisKeeperServer.getRedisMaster().partialState());
		sendMessageToMasterAndTestSlaveRedis();
	}

}

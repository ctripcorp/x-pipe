package com.ctrip.xpipe.redis.integratedtest.singledc;

import java.io.IOException;

import org.apache.commons.exec.ExecuteException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.exception.RedisSlavePromotionException;
import com.ctrip.xpipe.redis.meta.server.DefaultMetaServer;

/**
 * @author wenchao.meng
 *
 * Jun 13, 2016
 */
public class RedisFailOverTest extends AbstractSingleDcTest{
	
	
	@Test
	public void testRedisFailover() throws ExecuteException, IOException, RedisSlavePromotionException{
		
		RedisMeta redisMaster = getRedisMaster();
		stopServerListeningPort(redisMaster.getPort());
		
		RedisKeeperServer redisKeeperServer = getRedisKeeperServerActive();
		
		RedisMeta slave = getRedisSlaves().get(0);
		logger.info("[testRedisFailover][promote]{}:{}", slave.getIp(), slave.getPort());

		SERVER_ROLE role  = getRedisServerRole(slave);
		Assert.assertEquals(SERVER_ROLE.SLAVE, role);

		//TODO console ready delete this
		DefaultMetaServer metaServer = getDcInfo().getApplicationContext().getBean(DefaultMetaServer.class);
		metaServer.updateRedisMaster(slave.parent().parent().getId(), slave.parent().getId(), slave);
		redisKeeperServer.promoteSlave(slave.getIp(), slave.getPort());
		changeRedisMaster(redisMaster, slave);
		
		sleep(6000);
		role  = getRedisServerRole(slave);
		Assert.assertEquals(SERVER_ROLE.MASTER, role);
		
		Assert.assertEquals(PARTIAL_STATE.PARTIAL, redisKeeperServer.getRedisMaster().partialState());
		sendMessageToMasterAndTestSlaveRedis();
		
	}
	
	@After
	public void afterRedisFailOverTest() throws IOException{
	}
}

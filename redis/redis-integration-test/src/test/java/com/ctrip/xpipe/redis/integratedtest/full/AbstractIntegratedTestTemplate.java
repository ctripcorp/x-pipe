package com.ctrip.xpipe.redis.integratedtest.full;


import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.meta.server.impl.DefaultMetaServer;
import org.junit.Assert;

/**
 * @author wenchao.meng
 *
 * Jun 22, 2016
 */
public abstract class AbstractIntegratedTestTemplate extends AbstractFullIntegrated{
	
	
	@SuppressWarnings("unused")
	protected void failOverTestTemplate() throws Exception {
		
		RedisMeta redisMaster = getRedisMaster();
		stopServerListeningPort(redisMaster.getPort());
		
		RedisKeeperServer redisKeeperServer = getRedisKeeperServerActive(activeDc().getId());
		
		RedisMeta slave = getRedisSlaves(activeDc().getId()).get(0);

		SERVER_ROLE role  = getRedisServerRole(slave);
		Assert.assertEquals(SERVER_ROLE.SLAVE, role);

		logger.info(remarkableMessage("[testRedisFailover][promote]{}:{})"), slave.getIp(), slave.getPort());
		String clusterId = ((ClusterMeta) redisMaster.parent().parent()).getId();
		String shardId = redisMaster.parent().getId();
		DefaultMetaServer metaServer = getDcInfos().get(activeDc().getId()).getApplicationContext().getBean(DefaultMetaServer.class);
//		metaServer.promoteRedisMaster(clusterId, shardId, slave.getIp(), slave.getPort());

		sleep(6000);
		role  = getRedisServerRole(slave);
		Assert.assertEquals(SERVER_ROLE.MASTER, role);
		
		Assert.assertEquals(PARTIAL_STATE.PARTIAL, redisKeeperServer.getRedisMaster().partialState());
		changeRedisMaster(redisMaster, slave);
		sendMessageToMasterAndTestSlaveRedis();
	}

}

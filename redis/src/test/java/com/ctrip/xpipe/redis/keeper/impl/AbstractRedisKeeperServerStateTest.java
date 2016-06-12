package com.ctrip.xpipe.redis.keeper.impl;

import org.junit.Before;

import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.entity.RedisMeta;
import com.ctrip.xpipe.redis.keeper.meta.DefaultMetaServiceManager.MetaUpdateInfo;

/**
 * @author wenchao.meng
 *
 * Jun 12, 2016
 */
public class AbstractRedisKeeperServerStateTest extends AbstractRedisKeeperTest{
	
	protected RedisKeeperServer redisKeeperServer;

	protected RedisMeta redisMasterMeta;

	
	@Before
	public void beforeAbstractRedisKeeperServerStateTest() throws Exception{
		
		initRegistry();
		redisMasterMeta = createRedisMeta();
		redisKeeperServer = createRedisKeeperServer();
	}
	

	protected void update(Object args, AbstractRedisKeeperServerState state) {

		MetaUpdateInfo updateInfo = new MetaUpdateInfo(getClusterId(), getShardId(), args);
		
		state.update(updateInfo, null);
	}
}

package com.ctrip.xpipe.redis.keeper.impl;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.*;
import org.mockito.runners.MockitoJUnitRunner;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.simpleserver.Server;


/**
 * @author wenchao.meng
 *
 * Nov 15, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultRedisMasterReplicationTest extends AbstractRedisKeeperTest{
	
	private DefaultRedisMasterReplication  defaultRedisMasterReplication;
	private int replTimeoutSeconds = 1;
	
	@Mock
	private RedisMaster redisMaster;

	@Mock
	private RedisKeeperServer redisKeeperServer;

	@Before
	public void beforeDefaultRedisMasterReplicationTest() throws Exception{
		
		defaultRedisMasterReplication = new DefaultRedisMasterReplication(redisMaster, redisKeeperServer, scheduled, replTimeoutSeconds);
		when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
		add(defaultRedisMasterReplication);
	}
	
	@Test
	public void testTimeout() throws Exception{
		
		Server server = startEmptyServer();
		when(redisMaster.masterEndPoint()).thenReturn(new DefaultEndPoint("localhost", server.getPort()));
		defaultRedisMasterReplication.setMasterConnectRetryDelaySeconds(0);
		
		defaultRedisMasterReplication.initialize();
		defaultRedisMasterReplication.start();
		
		sleep(replTimeoutSeconds * 2000);
		
		verify(redisMaster, atLeast(2)).setMasterState(MASTER_STATE.REDIS_REPL_CONNECTING);
	}

}

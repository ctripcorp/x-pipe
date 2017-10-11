package com.ctrip.xpipe.redis.keeper.impl;


import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.redis.RunidGenerator;
import com.ctrip.xpipe.redis.core.store.MetaStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.simpleserver.Server;
import io.netty.channel.Channel;
import io.netty.channel.DefaultChannelPromise;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.*;


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
	
	@Mock
	private ReplicationStore replicationStore;
	@Mock
	private MetaStore metaStore;

	@Before
	public void beforeDefaultRedisMasterReplicationTest() throws Exception{
		
		defaultRedisMasterReplication = new DefaultRedisMasterReplication(redisMaster, redisKeeperServer, scheduled, replTimeoutSeconds);
		when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
		
		when(redisMaster.getCurrentReplicationStore()).thenReturn(replicationStore);
		when(replicationStore.getMetaStore()).thenReturn(metaStore);
		
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
	
	@Test
	public void testCancelScheduleWhenConnected() throws IOException{
		
		AtomicInteger replConfCount = new AtomicInteger();
		
		defaultRedisMasterReplication = new DefaultRedisMasterReplication(redisMaster, redisKeeperServer, scheduled, replTimeoutSeconds){
			@Override
			protected Command<Object> createReplConf() {
				replConfCount.incrementAndGet();
				return super.createReplConf();
			}
		};
		
		defaultRedisMasterReplication.onContinue(RunidGenerator.DEFAULT.generateRunid(), RunidGenerator.DEFAULT.generateRunid());
		
		Channel channel = mock(Channel.class);
		when(channel.closeFuture()).thenReturn(new DefaultChannelPromise(channel));
		
		defaultRedisMasterReplication.masterConnected(channel);
		
		int countBefore = replConfCount.get();
		
		sleep(DefaultRedisMasterReplication.REPLCONF_INTERVAL_MILLI * 2);
		
		int countAfter = replConfCount.get();
		
		Assert.assertEquals(countBefore, countAfter);
	}
}

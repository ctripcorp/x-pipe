package com.ctrip.xpipe.redis.keeper.impl;


import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.lifecycle.CreatedComponentRedistry;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
import com.ctrip.xpipe.redis.core.proxy.DefaultProxyProtocol;
import com.ctrip.xpipe.redis.core.proxy.DefaultProxyProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.*;
import com.ctrip.xpipe.redis.core.proxy.resource.KeeperProxyResourceManager;
import com.ctrip.xpipe.redis.core.redis.RunidGenerator;
import com.ctrip.xpipe.redis.core.store.MetaStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.container.ComponentRegistryHolder;
import com.ctrip.xpipe.simpleserver.Server;
import com.google.common.collect.Lists;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.ctrip.xpipe.redis.keeper.impl.AbstractRedisMasterReplication.KEY_MASTER_CONNECT_RETRY_DELAY_SECONDS;
import static org.mockito.Mockito.*;


/**
 * @author wenchao.meng
 *
 * Nov 15, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultRedisMasterReplicationTest extends AbstractRedisKeeperTest {

	private DefaultRedisMasterReplication defaultRedisMasterReplication;
	private int replTimeoutMilli = 200;

	@Mock
	private RedisMaster redisMaster;

	@Mock
	private RedisKeeperServer redisKeeperServer;

	@Mock
	private ReplicationStore replicationStore;

	@Mock
	private MetaStore metaStore;

	@Mock
	private ProxyResourceManager proxyResourceManager;

	private NioEventLoopGroup nioEventLoopGroup;

	@Before
	public void beforeDefaultRedisMasterReplicationTest() throws Exception {

		MockitoAnnotations.initMocks(this);

		nioEventLoopGroup = new NioEventLoopGroup();

		defaultRedisMasterReplication = new DefaultRedisMasterReplication(redisMaster, redisKeeperServer, nioEventLoopGroup,
				scheduled, replTimeoutMilli, proxyResourceManager);
		when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));

		when(redisMaster.getCurrentReplicationStore()).thenReturn(replicationStore);
		when(replicationStore.getMetaStore()).thenReturn(metaStore);

		add(defaultRedisMasterReplication);
	}

	@Test
	public void testStopReceivingDataWhenNotStarted() throws Exception {

		when(redisMaster.masterEndPoint()).thenReturn(new DefaultEndPoint("localhost", randomPort()));

		defaultRedisMasterReplication.initialize();
		try {
			defaultRedisMasterReplication.handleResponse(mock(Channel.class), Unpooled.wrappedBuffer(randomString().getBytes()));
			Assert.fail();
		}catch (RedisMasterReplicationStateException e){
			logger.info("{}", e.getMessage());
		}
		defaultRedisMasterReplication.start();

		Channel channel = mock(Channel.class);
		when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));

		defaultRedisMasterReplication.masterConnected(channel);
		defaultRedisMasterReplication.handleResponse(channel, Unpooled.wrappedBuffer(randomString().getBytes()));

		defaultRedisMasterReplication.stop();
		try {
			defaultRedisMasterReplication.handleResponse(mock(Channel.class), Unpooled.wrappedBuffer(randomString().getBytes()));
			Assert.fail();
		}catch (RedisMasterReplicationStateException e){
			logger.info("{}", e.getMessage());
		}
		defaultRedisMasterReplication.dispose();
		try {
			defaultRedisMasterReplication.handleResponse(mock(Channel.class), Unpooled.wrappedBuffer(randomString().getBytes()));
			Assert.fail();
		}catch (RedisMasterReplicationStateException e){
			logger.info("{}", e.getMessage());
		}


	}

	@Test
	public void testTimeout() throws Exception {

		Server server = startEmptyServer();
		when(redisMaster.masterEndPoint()).thenReturn(new DefaultEndPoint("localhost", server.getPort()));
		AtomicInteger connectingCount = new AtomicInteger(0);
		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				connectingCount.incrementAndGet();
				return null;
			}
		}).when(redisMaster).setMasterState(MASTER_STATE.REDIS_REPL_CONNECTING);
		
		defaultRedisMasterReplication.setMasterConnectRetryDelaySeconds(0);

		defaultRedisMasterReplication.initialize();
		defaultRedisMasterReplication.start();

		waitConditionUntilTimeOut(() -> connectingCount.get() >= 2, 3000);
	}

	@Test
	public void testCancelScheduleWhenConnected() throws IOException {

		AtomicInteger replConfCount = new AtomicInteger();

		defaultRedisMasterReplication = new DefaultRedisMasterReplication(redisMaster, redisKeeperServer,
				nioEventLoopGroup, scheduled, replTimeoutMilli, proxyResourceManager) {
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

	@Test
	public void testReconnectAfterTryConnectThroughException() throws Exception {
		System.setProperty(KEY_MASTER_CONNECT_RETRY_DELAY_SECONDS, "0");
		Server server = startEmptyServer();
		ProxyProtocol protocol = new DefaultProxyProtocolParser().read("PROXY ROUTE TCP://127.0.0.1:"+server.getPort());
		ProxyEnabledEndpoint endpoint = new ProxyEnabledEndpoint("127.0.0.1", server.getPort(), protocol);

		when(redisMaster.masterEndPoint()).thenReturn(endpoint);
		ProxyEndpointManager proxyEndpointManager = mock(ProxyEndpointManager.class);
		ProxyResourceManager proxyResourceManager = new KeeperProxyResourceManager(proxyEndpointManager, new NaiveNextHopAlgorithm());

		// first time empty list, sec time return endpoint
		when(proxyEndpointManager.getAvailableProxyEndpoints()).thenReturn(Lists.newArrayList())
				.thenReturn(protocol.nextEndpoints());
		defaultRedisMasterReplication = new DefaultRedisMasterReplication(redisMaster, redisKeeperServer,
				nioEventLoopGroup, scheduled, replTimeoutMilli, proxyResourceManager);

		defaultRedisMasterReplication = spy(defaultRedisMasterReplication);
		defaultRedisMasterReplication.initialize();
		defaultRedisMasterReplication.start();
		Thread.sleep(10);
		verify(defaultRedisMasterReplication, times(2)).connectWithMaster();
	}

	@After
	public void afterDefaultRedisMasterReplicationTest() throws Exception {
		nioEventLoopGroup.shutdownGracefully();
	}
}
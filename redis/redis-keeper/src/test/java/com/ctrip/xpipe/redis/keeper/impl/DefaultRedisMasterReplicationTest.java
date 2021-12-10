package com.ctrip.xpipe.redis.keeper.impl;


import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.proxy.ProxyEnabledEndpoint;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.endpoint.NaiveNextHopAlgorithm;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointManager;
import com.ctrip.xpipe.redis.core.redis.RunidGenerator;
import com.ctrip.xpipe.redis.core.store.MetaStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.utils.DefaultLeakyBucket;
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
import java.util.concurrent.TimeUnit;
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
	private KeeperResourceManager proxyResourceManager;

	private NioEventLoopGroup nioEventLoopGroup;

	private Server server;

	@Before
	public void beforeDefaultRedisMasterReplicationTest() throws Exception {

		MockitoAnnotations.initMocks(this);

		nioEventLoopGroup = new NioEventLoopGroup();
		server = startServer("+OK\r\n");

		defaultRedisMasterReplication = new DefaultRedisMasterReplication(redisMaster, redisKeeperServer, nioEventLoopGroup,
				scheduled, replTimeoutMilli, proxyResourceManager);
		when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));

		when(redisMaster.isKeeper()).thenReturn(false);
		when(redisMaster.getCurrentReplicationStore()).thenReturn(replicationStore);
		when(replicationStore.getMetaStore()).thenReturn(metaStore);
		KeeperMonitor keeperMonitor = createkeeperMonitor();
		when(redisKeeperServer.getKeeperMonitor()).thenReturn(keeperMonitor);
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

		waitConditionUntilTimeOut(() -> connectingCount.get() >= 2, 10000);
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
		ProxyConnectProtocol protocol = new DefaultProxyConnectProtocolParser().read("PROXY ROUTE TCP://127.0.0.1:"+server.getPort());
		ProxyEnabledEndpoint endpoint = new ProxyEnabledEndpoint("127.0.0.1", server.getPort(), protocol);

		when(redisMaster.masterEndPoint()).thenReturn(endpoint);
		ProxyEndpointManager proxyEndpointManager = mock(ProxyEndpointManager.class);
		KeeperResourceManager proxyResourceManager = new DefaultKeeperResourceManager(proxyEndpointManager, new NaiveNextHopAlgorithm(), new DefaultLeakyBucket(4));

		// first time empty list, sec time return endpoint
		when(proxyEndpointManager.getAvailableProxyEndpoints()).thenReturn(Lists.newArrayList())
				.thenReturn(protocol.nextEndpoints());
		defaultRedisMasterReplication = new DefaultRedisMasterReplication(redisMaster, redisKeeperServer,
				nioEventLoopGroup, scheduled, replTimeoutMilli, proxyResourceManager);

		defaultRedisMasterReplication = spy(defaultRedisMasterReplication);
		defaultRedisMasterReplication.initialize();
		defaultRedisMasterReplication.start();
		waitConditionUntilTimeOut(() ->
				mockingDetails(defaultRedisMasterReplication).getInvocations().stream()
						.filter(invocation -> invocation.getMethod().getName().equals("connectWithMaster"))
						.count() == 2);
	}

	@Test
	public void testConnectFailAndStopWaitClose() throws Exception {
		when(redisMaster.masterEndPoint()).thenReturn(new DefaultEndPoint("127.0.0.1", randomPort()));
		defaultRedisMasterReplication.initialize();
		defaultRedisMasterReplication.start(); // connect fail
		defaultRedisMasterReplication.stop();
		defaultRedisMasterReplication.dispose();
		defaultRedisMasterReplication.waitReplStopCompletely().get(1, TimeUnit.SECONDS);
	}

	@Test
	public void testStopAndDisconnectWaitClose() throws Exception {
		when(redisMaster.masterEndPoint()).thenReturn(new DefaultEndPoint("127.0.0.1", server.getPort()));
		defaultRedisMasterReplication.initialize();
		defaultRedisMasterReplication.start(); // connect success
		defaultRedisMasterReplication.waitReplConnected().addListener(f -> {
			// stop after connected
			defaultRedisMasterReplication.stop();
			defaultRedisMasterReplication.dispose();
		});
		defaultRedisMasterReplication.waitReplStopCompletely().get(1, TimeUnit.SECONDS);
	}

	@Test
	public void testStopAndConnectedWaitClose() throws Exception {
		when(redisMaster.masterEndPoint()).thenReturn(new DefaultEndPoint("127.0.0.1", server.getPort()));
		defaultRedisMasterReplication = spy(defaultRedisMasterReplication);
		defaultRedisMasterReplication.initialize();
		defaultRedisMasterReplication.start(); // connect success
		defaultRedisMasterReplication.stop();
		defaultRedisMasterReplication.dispose();
		defaultRedisMasterReplication.waitReplStopCompletely().get(1, TimeUnit.SECONDS);
		// one from stop and the other from masterConnected
		verify(defaultRedisMasterReplication, times(2)).disconnectWithMaster();

	}

	@After
	public void afterDefaultRedisMasterReplicationTest() throws Exception {
		nioEventLoopGroup.shutdownGracefully();
		server.stop();
	}
}
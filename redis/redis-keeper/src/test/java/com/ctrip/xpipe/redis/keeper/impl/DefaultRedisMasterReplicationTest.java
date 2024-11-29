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
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.utils.DefaultLeakyBucket;
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
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

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

	private int BACK_DEFAULT_REPLICATION_TIMEOUT_MILLI = AbstractRedisMasterReplication.DEFAULT_REPLICATION_TIMEOUT_MILLI;

	@Before
	public void beforeDefaultRedisMasterReplicationTest() throws Exception {

		MockitoAnnotations.initMocks(this);

		nioEventLoopGroup = new NioEventLoopGroup(1);

		AbstractRedisMasterReplication.DEFAULT_REPLICATION_TIMEOUT_MILLI = replTimeoutMilli;
		defaultRedisMasterReplication = new DefaultRedisMasterReplication(redisMaster, redisKeeperServer, nioEventLoopGroup,
				scheduled, proxyResourceManager);
		when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));

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
				nioEventLoopGroup, scheduled, proxyResourceManager) {
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
		DefaultEndPoint endpoint = new DefaultEndPoint("127.0.0.1", server.getPort());
		when(redisMaster.masterEndPoint()).thenReturn(endpoint);
		KeeperResourceManager proxyResourceManager = new DefaultKeeperResourceManager(new DefaultLeakyBucket(4));

		defaultRedisMasterReplication = new DefaultRedisMasterReplication(redisMaster, redisKeeperServer,
				nioEventLoopGroup, scheduled, proxyResourceManager);

		defaultRedisMasterReplication = spy(defaultRedisMasterReplication);
		defaultRedisMasterReplication.initialize();
		defaultRedisMasterReplication.start();
		waitConditionUntilTimeOut(() ->
				mockingDetails(defaultRedisMasterReplication).getInvocations().stream()
						.filter(invocation -> invocation.getMethod().getName().equals("connectWithMaster"))
						.count() == 2);
	}

	@Test
	public void testMultiReconnect() throws Exception {
		AbstractRedisMasterReplication.DEFAULT_REPLICATION_TIMEOUT_MILLI = 10000;
		Server server = startEmptyServer();
		when(redisMaster.masterEndPoint()).thenReturn(new DefaultEndPoint("127.0.0.1", server.getPort()));

		DefaultRedisMasterReplication mockReplication = new DefaultRedisMasterReplication(redisMaster, redisKeeperServer, nioEventLoopGroup,
				scheduled, proxyResourceManager) {
			@Override
			protected void doInitialize() throws Exception {
			}

			@Override
			protected void doStart() throws Exception {
			}
		};
		mockReplication.setMasterConnectRetryDelaySeconds(0);
		mockReplication.initialize();
		mockReplication.start();

		mockReplication.scheduleReconnect(0);
		mockReplication.scheduleReconnect(0);
		Thread.sleep(1000);
		Assert.assertEquals(1, server.getConnected());
	}

	@Test
	public void testMultiConnect() throws Exception {
		AbstractRedisMasterReplication.DEFAULT_REPLICATION_TIMEOUT_MILLI = 10000;
		Server server = startEmptyServer();
		when(redisMaster.masterEndPoint()).thenReturn(new DefaultEndPoint("127.0.0.1", server.getPort()));

		DefaultRedisMasterReplication mockReplication = new DefaultRedisMasterReplication(redisMaster, redisKeeperServer, nioEventLoopGroup,
				scheduled, proxyResourceManager) {
			@Override
			protected void doInitialize() throws Exception {
			}

			@Override
			protected void doStart() throws Exception {
			}
		};
		mockReplication.setMasterConnectRetryDelaySeconds(0);
		mockReplication.initialize();
		mockReplication.start();

		mockReplication.connectWithMaster();
		mockReplication.connectWithMaster();
		Thread.sleep(1000);
		Assert.assertEquals(1, server.getConnected());
	}

	@After
	public void afterDefaultRedisMasterReplicationTest() throws Exception {
		nioEventLoopGroup.shutdownGracefully();
		AbstractRedisMasterReplication.DEFAULT_REPLICATION_TIMEOUT_MILLI = BACK_DEFAULT_REPLICATION_TIMEOUT_MILLI;
	}
}
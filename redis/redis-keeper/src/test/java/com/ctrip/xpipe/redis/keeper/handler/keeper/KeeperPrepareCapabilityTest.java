package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractConfigCommand;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServerState;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.prepare.PrepareWatchSnapshot;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Phase EX (T-EX.4): CONFIG GET capability keys + INFO PREPARE snapshot. AC-12 / AC-2b / D11.
 */
@RunWith(MockitoJUnitRunner.class)
public class KeeperPrepareCapabilityTest {

	private static final String PREPARE_HOST = "10.0.0.1";

	private static final int PREPARE_PORT = 6380;

	private static final String MASTER_REPL_ID = "0123456789abcdef0123456789abcdef01234567";

	private static final String MASTER_REPL_ID2 = "fedcba9876543210fedcba9876543210fedcba98";

	@Mock
	private RedisClient redisClient;

	@Mock
	private RedisKeeperServer redisKeeperServer;

	@Mock
	private RedisKeeperServerState keeperServerState;

	@Mock
	private KeeperConfig keeperConfig;

	@Mock
	private ReplicationStore openedStore;

	private final ConfigHandler configHandler = new ConfigHandler();

	private final InfoHandler infoHandler = new InfoHandler();

	private final AtomicReference<Object> sent = new AtomicReference<>();

	@Before
	public void beforeKeeperPrepareCapabilityTest() {
		when(redisClient.getRedisServer()).thenReturn(redisKeeperServer);
		doAnswer(invocation -> {
			sent.set(invocation.getArguments()[0]);
			return null;
		}).when(redisClient).sendMessage(any(ByteBuf.class));
		doAnswer(invocation -> {
			sent.set(invocation.getArguments()[0]);
			return null;
		}).when(redisClient).sendMessage(any(byte[].class));
	}

	@Test
	public void testConfigGetPrepareWatchZeroWhenWatchDisabled() throws Exception {
		when(redisKeeperServer.getKeeperConfig()).thenReturn(keeperConfig);
		when(keeperConfig.isPrepareStoreWatchEnabled()).thenReturn(false);

		configHandler.doHandle(new String[]{"get", "prepare-watch"}, redisClient);

		assertConfigFlag(AbstractConfigCommand.REDIS_CONFIG_TYPE.PREPARE_WATCH.getConfigName(), "0");
	}

	@Test
	public void testConfigGetPrepareWatchZeroWhenNotTfs() throws Exception {
		when(redisKeeperServer.getKeeperConfig()).thenReturn(keeperConfig);
		when(keeperConfig.isPrepareStoreWatchEnabled()).thenReturn(true);
		when(redisKeeperServer.isTfsMode()).thenReturn(false);

		configHandler.doHandle(new String[]{"get", "prepare-watch"}, redisClient);

		assertConfigFlag(AbstractConfigCommand.REDIS_CONFIG_TYPE.PREPARE_WATCH.getConfigName(), "0");
	}

	@Test
	public void testConfigGetPrepareWatchOneWhenCapableEvenIfActive() throws Exception {
		when(redisKeeperServer.getKeeperConfig()).thenReturn(keeperConfig);
		when(keeperConfig.isPrepareStoreWatchEnabled()).thenReturn(true);
		when(redisKeeperServer.isTfsMode()).thenReturn(true);

		configHandler.doHandle(new String[]{"get", "prepare-watch"}, redisClient);

		assertConfigFlag(AbstractConfigCommand.REDIS_CONFIG_TYPE.PREPARE_WATCH.getConfigName(), "1");
		verify(redisKeeperServer, never()).getOpenedStore();
		verify(redisKeeperServer, never()).getReplicationStore();
		verify(redisKeeperServer, never()).getRedisKeeperServerState();
	}

	@Test
	public void testConfigGetPubsubParseFollowsSwitch() throws Exception {
		when(redisKeeperServer.getKeeperConfig()).thenReturn(keeperConfig);
		when(keeperConfig.isPubsubParseEnabled()).thenReturn(false);
		configHandler.doHandle(new String[]{"get", "pubsub-parse"}, redisClient);
		assertConfigFlag(AbstractConfigCommand.REDIS_CONFIG_TYPE.PUBSUB_PARSE.getConfigName(), "0");

		when(keeperConfig.isPubsubParseEnabled()).thenReturn(true);
		configHandler.doHandle(new String[]{"GET", "PUBSUB-PARSE"}, redisClient);
		assertConfigFlag(AbstractConfigCommand.REDIS_CONFIG_TYPE.PUBSUB_PARSE.getConfigName(), "1");
	}

	@Test
	public void testConfigGetUnknownRemainsEmptyArray() throws Exception {
		configHandler.doHandle(new String[]{"get", "mockconfig"}, redisClient);
		Assert.assertArrayEquals("*0\r\n".getBytes(StandardCharsets.UTF_8), (byte[]) sent.get());
	}

	@Test
	public void testConfigSetUnsupported() throws Exception {
		try {
			configHandler.doHandle(new String[]{"set", "prepare-watch", "1"}, redisClient);
			Assert.fail("CONFIG SET should not be supported");
		} catch (IllegalStateException e) {
			Assert.assertTrue(e.getMessage().contains("unknown command"));
		}
	}

	@Test
	public void testInfoPrepareUsesSnapshotAndKeepsState() {
		stubPrepareMaster();
		when(redisKeeperServer.slaves()).thenReturn(Collections.emptySet());
		when(redisKeeperServer.getOpenedStore()).thenReturn(openedStore);
		when(redisKeeperServer.getPrepareWatchSnapshot()).thenReturn(
				new PrepareWatchSnapshot(100L, 100L, 12345L, MASTER_REPL_ID, MASTER_REPL_ID2, 99L, 10L));

		infoHandler.doHandle(new String[]{"replication"}, redisClient);

		String body = ByteBufUtils.readToString((ByteBuf) sent.get());
		Assert.assertTrue(body.contains("state:PREPARE"));
		Assert.assertTrue(body.contains("master_host:" + PREPARE_HOST));
		Assert.assertTrue(body.contains("master_port:" + PREPARE_PORT));
		Assert.assertTrue(body.contains("slave_repl_offset:12345"));
		Assert.assertTrue(body.contains("master_repl_offset:12345"));
		Assert.assertTrue(body.contains("master_replid:" + MASTER_REPL_ID));
		Assert.assertTrue(body.contains("master_replid2:" + MASTER_REPL_ID2));
		Assert.assertTrue(body.contains("second_repl_offset:99"));
		Assert.assertTrue(body.contains("repl_backlog_first_byte_offset:10"));
		Assert.assertTrue(body.contains("repl_backlog_size:91"));
		Assert.assertTrue(body.contains("repl_backlog_histlen:91"));
		Assert.assertTrue(body.contains("repl_backlog_active:1"));
		Assert.assertTrue(body.contains("connected_slaves:0"));
		Assert.assertFalse(body.contains("master_link_status:up"));
		verifyNoStoreOpenOnCommandThread();
	}

	@Test
	public void testInfoPrepareConnectedSlavesFromServer() {
		stubPrepareMaster();
		RedisSlave slave = mock(RedisSlave.class);
		when(slave.info()).thenReturn("ip=10.0.0.8,port=6390,state=online,offset=1,lag=0,remotePort=4000");
		when(redisKeeperServer.slaves()).thenReturn(Collections.singleton(slave));
		when(redisKeeperServer.getOpenedStore()).thenReturn(openedStore);
		when(redisKeeperServer.getPrepareWatchSnapshot()).thenReturn(
				new PrepareWatchSnapshot(100L, 100L, 12345L, MASTER_REPL_ID, MASTER_REPL_ID2, -1L, 0L));

		infoHandler.doHandle(new String[]{"replication"}, redisClient);

		String body = ByteBufUtils.readToString((ByteBuf) sent.get());
		Assert.assertTrue(body.contains("connected_slaves:1"));
		Assert.assertTrue(body.contains("slave0:ip=10.0.0.8,port=6390"));
		verify(redisKeeperServer).slaves();
		verifyNoStoreOpenOnCommandThread();
	}

	@Test
	public void testInfoPrepareFallsBackWhenSnapshotMissing() {
		stubPrepareMaster();
		when(redisKeeperServer.getOpenedStore()).thenReturn(openedStore);
		when(redisKeeperServer.getPrepareWatchSnapshot()).thenReturn(null);

		infoHandler.doHandle(new String[]{"replication"}, redisClient);

		String body = ByteBufUtils.readToString((ByteBuf) sent.get());
		Assert.assertTrue(body.contains("state:PREPARE"));
		Assert.assertTrue(body.contains("slave_repl_offset:0"));
		Assert.assertTrue(body.contains("master_repl_offset:0"));
		Assert.assertTrue(body.contains("repl_backlog_active:0"));
		Assert.assertTrue(body.contains("repl_backlog_size:0"));
		verifyNoStoreOpenOnCommandThread();
	}

	private void stubPrepareMaster() {
		when(redisKeeperServer.role()).thenReturn(SERVER_ROLE.KEEPER);
		when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(keeperServerState);
		when(keeperServerState.keeperState()).thenReturn(KeeperState.PREPARE);
		when(keeperServerState.getMaster()).thenReturn(new DefaultEndPoint(PREPARE_HOST, PREPARE_PORT));
		when(redisKeeperServer.getRedisMaster()).thenReturn(null);
	}

	private void verifyNoStoreOpenOnCommandThread() {
		verify(redisKeeperServer, never()).getReplicationStore();
		verify(redisKeeperServer, never()).getKeeperRepl();
		verify(openedStore, never()).getMetaStore();
		verify(openedStore, never()).getCurReplStageReplOff();
		verify(openedStore, never()).backlogEndOffset();
		verify(openedStore, never()).backlogBeginOffset();
	}

	private void assertConfigFlag(String key, String expected) {
		String real = ByteBufUtils.readToString((ByteBuf) sent.get());
		Object[] payload = new ArrayParser().read(Unpooled.wrappedBuffer(real.getBytes(StandardCharsets.UTF_8))).getPayload();
		Assert.assertEquals(2, payload.length);
		Assert.assertEquals(key, String.valueOf(payload[0]));
		Assert.assertEquals(expected, String.valueOf(payload[1]));
	}
}

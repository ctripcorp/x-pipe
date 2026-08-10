package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServerState;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisClient;
import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateException;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperStats;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.*;

/**
 * D34 / Phase 17: PREPARE observation commands must not touch Store.
 */
@RunWith(MockitoJUnitRunner.class)
public class PrepareObservationHandlerTest extends AbstractRedisKeeperTest {

	@Mock
	private RedisKeeperServer redisKeeperServer;

	@Mock
	private RedisKeeperServerState keeperServerState;

	@Mock
	private KeeperMonitor keeperMonitor;

	@Mock
	private KeeperStats keeperStats;

	private final InfoHandler infoHandler = new InfoHandler();
	private final RoleCommandHandler roleHandler = new RoleCommandHandler();

	@Before
	public void beforePrepareObservationHandlerTest() {
		when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(keeperServerState);
		when(keeperServerState.keeperState()).thenReturn(KeeperState.PREPARE);
		when(keeperServerState.getMaster()).thenReturn(new DefaultEndPoint("10.0.0.2", 6379));
		when(redisKeeperServer.getRedisMaster()).thenReturn(null);
		when(redisKeeperServer.role()).thenReturn(Server.SERVER_ROLE.KEEPER);
		lenient().when(redisKeeperServer.getReplicationStore())
				.thenThrow(new RedisKeeperServerStateException("keeper", "PREPARE"));
		lenient().when(redisKeeperServer.getKeeperRepl())
				.thenThrow(new RedisKeeperServerStateException("keeper", "PREPARE"));
		lenient().when(redisKeeperServer.info()).thenReturn("os:test\r\nrun_id:abc\r\nuptime_in_seconds:1");
		lenient().when(redisKeeperServer.getKeeperMonitor()).thenReturn(keeperMonitor);
		lenient().when(keeperMonitor.getKeeperStats()).thenReturn(keeperStats);
	}

	private String invokeInfo(String... args) throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel();
		RedisClient<?> client = new DefaultRedisClient(channel, redisKeeperServer);
		infoHandler.handle(args, client);
		Object outbound = channel.readOutbound();
		Assert.assertNotNull(outbound);
		Assert.assertTrue(outbound instanceof ByteBuf);
		ByteBuf buf = (ByteBuf) outbound;
		try {
			String raw = ByteBufUtils.readToString(buf.duplicate());
			// bulk string: $<len>\r\n<body>\r\n
			int idx = raw.indexOf("\r\n");
			Assert.assertTrue(raw.startsWith("$"));
			return raw.substring(idx + 2, raw.length() - 2);
		} finally {
			buf.release();
		}
	}

	private String invokeRole() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel();
		RedisClient<?> client = new DefaultRedisClient(channel, redisKeeperServer);
		roleHandler.handle(new String[0], client);
		Object outbound = channel.readOutbound();
		Assert.assertNotNull(outbound);
		Assert.assertTrue(outbound instanceof ByteBuf);
		ByteBuf buf = (ByteBuf) outbound;
		try {
			return ByteBufUtils.readToString(buf.duplicate());
		} finally {
			buf.release();
		}
	}

	@Test
	public void testInfoReplicationPrepareContainsStateAndSkipsStore() throws Exception {
		String info = invokeInfo("replication");
		Assert.assertTrue(info.contains("state:" + KeeperState.PREPARE));
		Assert.assertTrue(info.contains("role:slave"));
		Assert.assertTrue(info.contains("slave_priority:0"));
		Assert.assertTrue(info.contains("connected_slaves:0"));
		Assert.assertFalse(info.contains("master_link_status:up"));
		Assert.assertFalse(info.contains("currentState:PREPARE"));

		InfoResultExtractor extractor = new InfoResultExtractor(info);
		Assert.assertEquals(KeeperState.PREPARE.name(), extractor.getKeeperState());

		verify(redisKeeperServer, never()).getReplicationStore();
		verify(redisKeeperServer, never()).getKeeperRepl();
	}

	@Test
	public void testInfoAllPrepareSucceedsWithoutStore() throws Exception {
		String info = invokeInfo("all");
		Assert.assertTrue(info.contains("state:" + KeeperState.PREPARE));
		Assert.assertTrue(info.contains("# Gtid") || info.contains("# gtid") || info.toLowerCase().contains("gtid"));
		verify(redisKeeperServer, never()).getReplicationStore();
	}

	@Test
	public void testRolePrepareLegalTupleWithoutStore() throws Exception {
		String real = invokeRole();
		Object[] reverse = new ArrayParser().read(Unpooled.wrappedBuffer(real.getBytes())).getPayload();
		SlaveRole slaveRole = new SlaveRole(reverse);
		Assert.assertEquals(Server.SERVER_ROLE.KEEPER, slaveRole.getServerRole());
		Assert.assertEquals("10.0.0.2", slaveRole.getMasterHost());
		Assert.assertEquals(6379, slaveRole.getMasterPort());
		Assert.assertEquals(MASTER_STATE.REDIS_REPL_NONE, slaveRole.getMasterState());
		Assert.assertEquals(-1L, slaveRole.getMasterOffset());
		Assert.assertFalse(real.contains(MASTER_STATE.REDIS_REPL_CONNECTED.getDesc()));
		verify(redisKeeperServer, never()).getReplicationStore();
	}
}

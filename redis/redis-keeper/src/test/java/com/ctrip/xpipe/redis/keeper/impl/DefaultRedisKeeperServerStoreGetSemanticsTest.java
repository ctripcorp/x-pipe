package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperContextTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServerState;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.handler.AbstractCommandHandler;
import com.ctrip.xpipe.redis.keeper.handler.keeper.GtidxHandler;
import com.ctrip.xpipe.redis.keeper.handler.keeper.InfoHandler;
import com.ctrip.xpipe.redis.keeper.handler.keeper.KinfoCommandHandler;
import com.ctrip.xpipe.redis.keeper.handler.keeper.RoleCommandHandler;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

/**
 * m6 spec §9 (C7, T-P11.6): queries and ops entries use get semantics and never construct a ReplicationStore;
 * data sync / state transition keep getOrCreate.
 */
public class DefaultRedisKeeperServerStoreGetSemanticsTest extends AbstractRedisKeeperContextTest {

	private static final String UUID = "a4f566ef50a85e1119f17f9b746728b48609a2ab";

	/** Store-derived INFO replication fields that must be omitted when no store is opened. */
	private static final String[] STORE_FIELDS = {"slave_repl_offset:", "master_replid:", "master_replid2:",
			"master_repl_offset:", "second_repl_offset:", "repl_backlog_", "rordb_", "rdb_"};

	private DefaultRedisKeeperServer server;

	private ReplicationStoreManager manager;

	@Before
	public void beforeDefaultRedisKeeperServerStoreGetSemanticsTest() throws Exception {
		// Fresh keeper: no latest.store.dir, initKeeperServerState finds nothing to open
		server = (DefaultRedisKeeperServer) createRedisKeeperServer();
		server.initialize();
		server.start();
		manager = spy(server.getReplicationStoreManager());
		server.setReplicationStoreManager(manager);
		Assert.assertNull(manager.getOpenedStore());
	}

	@After
	public void afterDefaultRedisKeeperServerStoreGetSemanticsTest() throws Exception {
		if (server != null) {
			server.stop();
			server.dispose();
		}
	}

	@Test
	public void testQueriesOnFreshKeeperDoNotConstructStore() throws Exception {
		long createCount = createCount();

		String replication = invoke(new InfoHandler(), "replication");
		Assert.assertTrue(replication.contains("state:" + KeeperState.UNKNOWN));
		assertNoStoreFields(replication);
		String gtid = invoke(new InfoHandler(), "gtid");
		Assert.assertFalse(gtid, gtid.contains("gtid_executed"));
		String all = invoke(new InfoHandler());
		Assert.assertFalse(all, all.contains("master_repl_offset:"));
		Assert.assertFalse(all, all.contains("gtid_executed"));

		Object[] payload = new ArrayParser().read(Unpooled.wrappedBuffer(invoke(new RoleCommandHandler()).getBytes()))
				.getPayload();
		Assert.assertEquals(-1L, new SlaveRole(payload).getMasterOffset());

		String kinfo = invoke(new KinfoCommandHandler());
		Assert.assertTrue(kinfo, kinfo.startsWith("-"));
		Assert.assertTrue(kinfo, kinfo.contains(RedisKeeperServer.ERR_REPLICATION_STORE_NOT_OPENED));
		Assert.assertTrue(invoke(new GtidxHandler(), "add", "executed", UUID, "1", "2")
				.contains("ERR " + RedisKeeperServer.ERR_REPLICATION_STORE_NOT_OPENED));
		Assert.assertTrue(invoke(new GtidxHandler(), "remove", "lost", UUID, "1", "2")
				.contains("ERR " + RedisKeeperServer.ERR_REPLICATION_STORE_NOT_OPENED));

		server.releaseRdb();
		CommandFuture<?> searched = server.createCmdKeySearcher(UUID, 1, 2).execute();
		searched.await(5, TimeUnit.SECONDS);
		Assert.assertFalse(searched.isSuccess());
		Assert.assertTrue(searched.cause().getMessage().contains(RedisKeeperServer.ERR_REPLICATION_STORE_NOT_OPENED));

		Assert.assertNull(server.getReplicationStore());
		Assert.assertNull(server.getKeeperRepl());
		verify(manager, never()).getCurrent();
		verify(manager, never()).createIfNotExist();
		verify(manager, never()).create();
		Assert.assertNull(manager.getOpenedStore());
		Assert.assertEquals(createCount, createCount());
	}

	@Test
	public void testOpenedStoreServesQueriesAndDataPathStillCreates() throws Exception {
		long createCount = createCount();
		ReplicationStore store = server.getOrCreateReplicationStore();
		Assert.assertNotNull(store);
		Assert.assertEquals("getOrCreate keeps creating a store on a fresh keeper", createCount + 1, createCount());
		Assert.assertSame(store, server.getReplicationStore());
		// INFO on a store without repl stage NPEs in KeeperRepl (pre-existing, unrelated to m6); seed one
		store.psyncContinueFrom(UUID, 1);

		clearInvocations(manager);
		String replication = invoke(new InfoHandler(), "replication");
		Assert.assertTrue(replication, replication.contains("master_repl_offset:"));
		Assert.assertTrue(replication, replication.contains("slave_repl_offset:"));
		Assert.assertTrue(invoke(new KinfoCommandHandler()).startsWith("$"));
		verify(manager, never()).getCurrent();
		verify(manager, never()).createIfNotExist();
	}

	@Test
	public void testClosedStoreIsTreatedAsNotOpened() throws Exception {
		ReplicationStore store = server.getOrCreateReplicationStore();
		store.close();
		clearInvocations(manager);

		Assert.assertNull(server.getReplicationStore());
		assertNoStoreFields(invoke(new InfoHandler(), "replication"));
		verify(manager, never()).getCurrent();
		verify(manager, never()).createIfNotExist();
	}

	@Test
	public void testStateTransitionStillCreatesStore() throws Exception {
		long createCount = createCount();
		server.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("127.0.0.1", randomPort()));
		Assert.assertEquals(KeeperState.ACTIVE, server.getRedisKeeperServerState().keeperState());
		Assert.assertNotNull(server.getReplicationStore());
		Assert.assertEquals(createCount + 1, createCount());
	}

	/** Master link fields follow getRedisMaster(), independent of whether a store is opened (spec §9.4). */
	@Test
	public void testInfoReplicationWithoutStoreKeepsMasterLink() throws Exception {
		RedisKeeperServer mockServer = mock(RedisKeeperServer.class);
		RedisKeeperServerState state = mock(RedisKeeperServerState.class);
		RedisMaster redisMaster = mock(RedisMaster.class);
		when(mockServer.getRedisKeeperServerState()).thenReturn(state);
		when(state.keeperState()).thenReturn(KeeperState.ACTIVE);
		when(mockServer.role()).thenReturn(Server.SERVER_ROLE.KEEPER);
		when(mockServer.getRedisMaster()).thenReturn(redisMaster);
		when(redisMaster.masterEndPoint()).thenReturn(new DefaultEndPoint("10.0.0.1", 6379));
		when(mockServer.slaves()).thenReturn(Collections.emptySet());
		when(mockServer.getReplicationStore()).thenReturn(null);

		String info = invoke(new InfoHandler(), mockServer, "replication");
		Assert.assertTrue(info, info.contains("master_host:10.0.0.1"));
		Assert.assertTrue(info, info.contains("master_port:6379"));
		Assert.assertTrue(info, info.contains("master_link_status:up"));
		Assert.assertTrue(info, info.contains("connected_slaves:0"));
		assertNoStoreFields(info);
		verify(mockServer, never()).getOrCreateReplicationStore();
		verify(mockServer, never()).getOrCreateKeeperRepl();

		when(mockServer.getRedisMaster()).thenReturn(null);
		String noMaster = invoke(new InfoHandler(), mockServer, "replication");
		Assert.assertFalse(noMaster, noMaster.contains("master_host:"));
		Assert.assertFalse(noMaster, noMaster.contains("master_link_status:"));
	}

	@Override
	protected String getXpipeMetaConfigFile() {
		return "keeper-test.xml";
	}

	private void assertNoStoreFields(String info) {
		for (String field : STORE_FIELDS) {
			Assert.assertFalse(field + " must be omitted: " + info, info.contains(field));
		}
	}

	private long createCount() {
		return server.getKeeperMonitor().getReplicationStoreStats().getReplicationStoreCreateCount();
	}

	private String invoke(AbstractCommandHandler handler, String... args) throws Exception {
		return invoke(handler, server, args);
	}

	private String invoke(AbstractCommandHandler handler, RedisKeeperServer target, String... args) throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel();
		handler.handle(args, new DefaultRedisClient(channel, target));
		ByteBuf buf = channel.readOutbound();
		Assert.assertNotNull(buf);
		try {
			return ByteBufUtils.readToString(buf.duplicate());
		} finally {
			buf.release();
		}
	}
}

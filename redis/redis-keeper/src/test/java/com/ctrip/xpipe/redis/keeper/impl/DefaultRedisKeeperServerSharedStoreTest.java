package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperContextTest;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.handler.keeper.InfoHandler;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;

/**
 * TFS: every keeper of a shard sees the same store dir. A keeper must not open it RW, create a
 * store, delete files or gc before MetaServer makes it the slot holder.
 */
public class DefaultRedisKeeperServerSharedStoreTest extends AbstractRedisKeeperContextTest {

	private File sharedBaseDir;

	private String runid;

	@Before
	public void beforeDefaultRedisKeeperServerSharedStoreTest() {
		sharedBaseDir = new File(getTestFileDir(), "tfs_shared");
		// keepers of one dc-shard share a runid (RedisDao reuses it), so the store meta accepts both
		runid = randomString(40);
	}

	/** Scenario 1: holder A writes; keeper B (re)starts on the same dir. B must not touch anything. */
	@Test
	public void testBootDoesNotTouchSharedStore() throws Exception {
		DefaultRedisKeeperServer holder = startTfsKeeper();
		DefaultRedisKeeperServer restarted = null;
		try {
			becomeActive(holder);
			File managerDir = holder.getReplicationStoreManager().getBaseDir();
			File storeDir = storeBaseDir(holder.getReplicationStore());
			byte[] metaBefore = managerMeta(managerDir);
			String[] dirsBefore = managerDir.list();

			restarted = startTfsKeeper();

			Assert.assertEquals(KeeperState.UNKNOWN, restarted.getRedisKeeperServerState().keeperState());
			Assert.assertNull("boot must not open the shared store", restarted.getOpenedStore());
			Assert.assertFalse(restarted.getReplicationStoreManager().isStoreWriteOwner());
			try {
				restarted.getReplicationStore();
				Assert.fail("non slot holder must not open the shared store");
			} catch (RuntimeException expected) {
				logger.info("[testBootDoesNotTouchSharedStore] expected {}", expected.getMessage());
			}

			Assert.assertArrayEquals("latest.store.dir unchanged", metaBefore, managerMeta(managerDir));
			Assert.assertEquals("no new UUID dir", sorted(dirsBefore), sorted(managerDir.list()));
			Assert.assertTrue(storeDir.isDirectory());
			Assert.assertTrue("holder still writes", holder.getReplicationStore().checkOk());
		} finally {
			stopQuietly(restarted);
			stopQuietly(holder);
		}
	}

	/** Scenario 2: INFO / health check on an UNKNOWN TFS keeper must not open or create() the store. */
	@Test
	public void testInfoOnUnknownDoesNotCreateStore() throws Exception {
		DefaultRedisKeeperServer keeper = startTfsKeeper();
		try {
			EmbeddedChannel channel = new EmbeddedChannel();
			new InfoHandler().handle(new String[]{"all"}, new DefaultRedisClient(channel, keeper));

			String info = readAll(channel);
			Assert.assertTrue(info, info.contains("state:" + KeeperState.UNKNOWN));
			Assert.assertNull(keeper.getOpenedStore());
			File managerDir = keeper.getReplicationStoreManager().getBaseDir();
			Assert.assertFalse("INFO must not create store_manager_meta",
					new File(managerDir, "store_manager_meta.properties").exists());
		} finally {
			stopQuietly(keeper);
		}
	}

	/** HTTP ops entries (releaseRdb, gtid key search) on an UNKNOWN TFS keeper fail instead of opening the store. */
	@Test
	public void testOpsEntriesOnUnknownDoNotCreateStore() throws Exception {
		DefaultRedisKeeperServer keeper = startTfsKeeper();
		try {
			try {
				keeper.releaseRdb();
				Assert.fail("releaseRdb must not open the shared store");
			} catch (IllegalStateException expected) {
				Assert.assertTrue(expected.getMessage(), expected.getMessage().startsWith("store not opened"));
			}
			try {
				keeper.createCmdKeySearcher("uuid", 1, 2).execute().get(5, java.util.concurrent.TimeUnit.SECONDS);
				Assert.fail("gtid key search must not open the shared store");
			} catch (java.util.concurrent.ExecutionException expected) {
				Assert.assertEquals("store not opened", expected.getCause().getMessage());
			}

			Assert.assertNull(keeper.getOpenedStore());
			Assert.assertFalse(new File(keeper.getReplicationStoreManager().getBaseDir(),
					"store_manager_meta.properties").exists());
		} finally {
			stopQuietly(keeper);
		}
	}

	/**
	 * Gtid key search on the slot holder whose store is closed: getReplicationStore() would see
	 * getCurrent() == null and create() a new UUID dir, switching latest.store.dir. The search must fail instead.
	 */
	@Test
	public void testSearchOnClosedStoreDoesNotCreateStore() throws Exception {
		DefaultRedisKeeperServer holder = startTfsKeeper();
		try {
			becomeActive(holder);
			ReplicationStore store = holder.getReplicationStore();
			File managerDir = holder.getReplicationStoreManager().getBaseDir();
			byte[] metaBefore = managerMeta(managerDir);
			String[] dirsBefore = managerDir.list();
			store.close();

			try {
				holder.createCmdKeySearcher("uuid", 1, 2).execute().get(5, java.util.concurrent.TimeUnit.SECONDS);
				Assert.fail("search on a closed store must fail");
			} catch (java.util.concurrent.ExecutionException expected) {
				Assert.assertEquals("store not opened", expected.getCause().getMessage());
			}

			Assert.assertSame("search must not replace the store", store, holder.getOpenedStore());
			Assert.assertArrayEquals("latest.store.dir unchanged", metaBefore, managerMeta(managerDir));
			Assert.assertEquals("no new UUID dir", sorted(dirsBefore), sorted(managerDir.list()));
		} finally {
			stopQuietly(holder);
		}
	}

	/**
	 * Holder whose store was released (currentStore == null, the state release / create leave behind): HTTP ops
	 * entries fail instead of reopening the store on the ops thread.
	 */
	@Test
	public void testOpsEntriesOnReleasedStoreDoNotReopen() throws Exception {
		DefaultRedisKeeperServer holder = startTfsKeeper();
		try {
			becomeActive(holder);
			holder.getReplicationStore();
			File managerDir = holder.getReplicationStoreManager().getBaseDir();
			byte[] metaBefore = managerMeta(managerDir);
			String[] dirsBefore = managerDir.list();
			holder.getReplicationStoreManager().releaseCurrentStore();
			Assert.assertTrue(holder.getReplicationStoreManager().isStoreWriteOwner());

			try {
				holder.releaseRdb();
				Assert.fail("releaseRdb must not reopen the store");
			} catch (IllegalStateException expected) {
				Assert.assertTrue(expected.getMessage(), expected.getMessage().startsWith("store not opened"));
			}
			try {
				holder.createCmdKeySearcher("uuid", 1, 2).execute().get(5, java.util.concurrent.TimeUnit.SECONDS);
				Assert.fail("search must not reopen the store");
			} catch (java.util.concurrent.ExecutionException expected) {
				Assert.assertEquals("store not opened", expected.getCause().getMessage());
			}

			Assert.assertNull("ops entries must not reopen the store", holder.getOpenedStore());
			Assert.assertArrayEquals("latest.store.dir unchanged", metaBefore, managerMeta(managerDir));
			Assert.assertEquals("no new UUID dir", sorted(dirsBefore), sorted(managerDir.list()));
		} finally {
			stopQuietly(holder);
		}
	}

	@Test
	public void testReleaseRdbOnOpenedStore() throws Exception {
		DefaultRedisKeeperServer holder = startTfsKeeper();
		try {
			becomeActive(holder);
			ReplicationStore store = holder.getReplicationStore();
			holder.releaseRdb();
			Assert.assertSame(store, holder.getOpenedStore());
		} finally {
			stopQuietly(holder);
		}
	}

	/** Scenario 3: old holder released; SETSTATE ACTIVE reopens the same latest store (no new UUID). */
	@Test
	public void testSetStateActiveReopensSharedStore() throws Exception {
		DefaultRedisKeeperServer oldHolder = startTfsKeeper();
		File storeDir;
		try {
			becomeActive(oldHolder);
			storeDir = storeBaseDir(oldHolder.getReplicationStore());
		} finally {
			// release before grant: models PREPARE / ForceCloseDir of the old holder
			stopQuietly(oldHolder);
		}

		DefaultRedisKeeperServer newHolder = startTfsKeeper();
		try {
			Assert.assertNull(newHolder.getOpenedStore());
			becomeActive(newHolder);

			Assert.assertEquals(KeeperState.ACTIVE, newHolder.getRedisKeeperServerState().keeperState());
			Assert.assertTrue(newHolder.getReplicationStoreManager().isStoreWriteOwner());
			Assert.assertEquals("must reopen the shared latest store", storeDir, storeBaseDir(newHolder.getReplicationStore()));
		} finally {
			stopQuietly(newHolder);
		}
	}

	/** Scenario 4: removing one keeper (container remove → destroy) must keep the shard's shared data. */
	@Test
	public void testDestroyKeepsSharedStore() throws Exception {
		DefaultRedisKeeperServer keeper = startTfsKeeper();
		File storeDir;
		try {
			becomeActive(keeper);
			storeDir = storeBaseDir(keeper.getReplicationStore());
		} finally {
			stopQuietly(keeper);
		}
		keeper.destroy();
		Assert.assertTrue(storeDir.isDirectory());
	}

	private DefaultRedisKeeperServer startTfsKeeper() throws Exception {
		KeeperMeta keeperMeta = createKeeperMeta(randomPort(), runid);
		DefaultRedisKeeperServer server = (DefaultRedisKeeperServer) createRedisKeeperServer(getReplId().id(), keeperMeta,
				new TestKeeperConfig(), sharedBaseDir, getRegistry().getComponent(LeaderElectorManager.class), true);
		server.initialize();
		server.start();
		return server;
	}

	private void becomeActive(DefaultRedisKeeperServer server) {
		server.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("127.0.0.1", randomPort()));
	}

	private static File storeBaseDir(ReplicationStore store) {
		return new File(store.toString().substring("ReplicationStore:".length()));
	}

	private static byte[] managerMeta(File managerDir) throws Exception {
		return Files.readAllBytes(new File(managerDir, "store_manager_meta.properties").toPath());
	}

	private static String[] sorted(String[] names) {
		String[] copy = names == null ? new String[0] : names.clone();
		Arrays.sort(copy);
		return copy;
	}

	private static String readAll(EmbeddedChannel channel) {
		StringBuilder sb = new StringBuilder();
		Object msg;
		while ((msg = channel.readOutbound()) != null) {
			if (msg instanceof ByteBuf) {
				ByteBuf buf = (ByteBuf) msg;
				sb.append(buf.toString(StandardCharsets.UTF_8));
				buf.release();
			} else {
				sb.append(msg);
			}
		}
		return sb.toString();
	}

	@Override
	protected String getXpipeMetaConfigFile() {
		return "keeper-test.xml";
	}

	private void stopQuietly(DefaultRedisKeeperServer server) {
		if (server == null) {
			return;
		}
		try {
			server.stop();
		} catch (Exception e) {
			logger.info("[stopQuietly] stop", e);
		}
		try {
			server.dispose();
		} catch (Exception e) {
			logger.info("[stopQuietly] dispose", e);
		}
	}
}

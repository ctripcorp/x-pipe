package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.mockito.Mockito.mock;

/**
 * Shared (TFS) store: only the write owner may open RW / create / gc / destroy the shared dir.
 */
public class DefaultReplicationStoreManagerSharedStoreTest extends AbstractRedisKeeperTest {

	private TestKeeperConfig keeperConfig;

	private File keeperBaseDir;

	private String runid;

	@Before
	public void beforeDefaultReplicationStoreManagerSharedStoreTest() {
		keeperConfig = new TestKeeperConfig();
		// gc is driven by hand; the scheduled one must not fire during a test
		keeperConfig.setReplicationStoreGcIntervalSeconds(3600);
		keeperConfig.setMinTimeMilliToGcAfterCreate(0);
		keeperBaseDir = new File(getTestFileDir());
		runid = randomKeeperRunid();
	}

	@Test
	public void testNonOwnerNeverOpensOrCreates() throws Exception {
		DefaultReplicationStoreManager manager = newSharedManager(createTestAsyncFileSystem());
		LifecycleHelper.initializeIfPossible(manager);
		try {
			Assert.assertFalse(manager.isStoreWriteOwner());
			Assert.assertNull(manager.getCurrent());
			assertNotOwner(manager::create);
			assertNotOwner(manager::createIfNotExist);
			Assert.assertNull(manager.getOpenedStore());
			// not even store_manager_meta.properties was opened RW (that would mkdir + create it)
			Assert.assertFalse(new File(manager.getBaseDir(), "store_manager_meta.properties").exists());
		} finally {
			LifecycleHelper.disposeIfPossible(manager);
		}
	}

	@Test
	public void testOwnerReopensSameStoreAfterGrant() throws Exception {
		AsyncFileSystem fs = createTestAsyncFileSystem();
		DefaultReplicationStoreManager holder = newSharedManager(fs);
		LifecycleHelper.initializeIfPossible(holder);
		holder.setStoreWriteOwner(true);
		File storeDir = storeBaseDir(holder.create());
		LifecycleHelper.disposeIfPossible(holder);

		DefaultReplicationStoreManager next = newSharedManager(fs);
		LifecycleHelper.initializeIfPossible(next);
		try {
			Assert.assertNull("no open before ownership", next.getCurrent());
			next.setStoreWriteOwner(true);
			ReplicationStore reopened = next.createIfNotExist();
			Assert.assertEquals("must reopen latest, not create a new UUID", storeDir, storeBaseDir(reopened));
		} finally {
			LifecycleHelper.disposeIfPossible(next);
		}
	}

	@Test
	public void testGcOnlyDeletesWhileOwner() throws Exception {
		DefaultReplicationStoreManager manager = newSharedManager(createTestAsyncFileSystem());
		LifecycleHelper.initializeIfPossible(manager);
		LifecycleHelper.startIfPossible(manager);
		try {
			manager.setStoreWriteOwner(true);
			File oldDir = storeBaseDir(manager.create());
			manager.create();
			sleep(10);

			manager.setStoreWriteOwner(false);
			manager.gc();
			Assert.assertTrue("non-owner gc must not delete", oldDir.exists());

			manager.setStoreWriteOwner(true);
			manager.gc();
			Assert.assertFalse("owner gc deletes old store", oldDir.exists());
		} finally {
			LifecycleHelper.stopIfPossible(manager);
			LifecycleHelper.disposeIfPossible(manager);
		}
	}

	@Test
	public void testDestroyKeepsSharedDir() throws Exception {
		DefaultReplicationStoreManager manager = newSharedManager(createTestAsyncFileSystem());
		LifecycleHelper.initializeIfPossible(manager);
		manager.setStoreWriteOwner(true);
		File storeDir = storeBaseDir(manager.create());

		manager.destroy();

		Assert.assertTrue("shared baseDir must survive one keeper's remove", manager.getBaseDir().isDirectory());
		Assert.assertTrue(storeDir.isDirectory());
	}

	@Test
	public void testPrivateStoreUnchanged() throws Exception {
		DefaultReplicationStoreManager manager = newManager(createTestAsyncFileSystem());
		LifecycleHelper.initializeIfPossible(manager);
		Assert.assertTrue("private store is always owned", manager.isStoreWriteOwner());
		Assert.assertNotNull(manager.createIfNotExist());
		manager.destroy();
		Assert.assertFalse(manager.getBaseDir().exists());
	}

	private DefaultReplicationStoreManager newSharedManager(AsyncFileSystem fs) {
		DefaultReplicationStoreManager manager = newManager(fs);
		manager.setSharedStore(true);
		return manager;
	}

	private DefaultReplicationStoreManager newManager(AsyncFileSystem fs) {
		return new DefaultReplicationStoreManager(
				keeperConfig, getReplId(), runid, keeperBaseDir,
				createkeeperMonitor(), mock(SyncRateManager.class), createRedisOpParser(), null, fs);
	}

	private static File storeBaseDir(ReplicationStore store) {
		return new File(store.toString().substring("ReplicationStore:".length()));
	}

	private static void assertNotOwner(ThrowingRunnable action) throws Exception {
		try {
			action.run();
			Assert.fail("expected IllegalStateException(not store write owner)");
		} catch (IllegalStateException e) {
			Assert.assertTrue(e.getMessage(), e.getMessage().startsWith(DefaultReplicationStoreManager.NOT_STORE_WRITE_OWNER_MSG));
		}
	}

	@FunctionalInterface
	private interface ThrowingRunnable {
		Object run() throws Exception;
	}
}

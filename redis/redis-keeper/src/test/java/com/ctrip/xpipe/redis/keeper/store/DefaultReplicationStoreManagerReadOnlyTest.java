package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.storage.AbstractStorageFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.*;

/**
 * Phase RM (T-RM.4): Manager read-only mode. AC-2 / AC-2c.
 */
public class DefaultReplicationStoreManagerReadOnlyTest extends AbstractRedisKeeperTest {

	private TestKeeperConfig keeperConfig;

	@Before
	public void beforeDefaultReplicationStoreManagerReadOnlyTest() {
		keeperConfig = new TestKeeperConfig();
		keeperConfig.setReplicationStoreGcIntervalSeconds(1);
		keeperConfig.setMinTimeMilliToGcAfterCreate(3000);
	}

	@Test
	public void testSetReadOnlyThrowsWhenStarted() throws Exception {
		DefaultReplicationStoreManager manager = newManager(createTestAsyncFileSystem());
		LifecycleHelper.initializeIfPossible(manager);
		LifecycleHelper.startIfPossible(manager);
		try {
			Assert.assertFalse(manager.isReadOnly());
			try {
				manager.setReadOnly(true);
				Assert.fail("setReadOnly must refuse when started");
			} catch (IllegalStateException e) {
				Assert.assertTrue(e.getMessage().contains("not started"));
			}
			Assert.assertFalse(manager.isReadOnly());
		} finally {
			LifecycleHelper.stopIfPossible(manager);
			LifecycleHelper.disposeIfPossible(manager);
		}
	}

	@Test
	public void testSetReadOnlyThrowsWhenCurrentStoreOpen() throws Exception {
		DefaultReplicationStoreManager manager = newManager(createTestAsyncFileSystem());
		LifecycleHelper.initializeIfPossible(manager);
		try {
			Assert.assertNotNull(manager.create());
			try {
				manager.setReadOnly(true);
				Assert.fail("setReadOnly must refuse when currentStore != null");
			} catch (IllegalStateException e) {
				Assert.assertTrue(e.getMessage().contains("currentStore"));
			}
		} finally {
			LifecycleHelper.disposeIfPossible(manager);
		}
	}

	@Test
	public void testSetReadOnlyClearsStaleCurrentMetaOnReenter() throws Exception {
		AsyncFileSystem fs = createTestAsyncFileSystem();
		DefaultReplicationStoreManager manager = newManager(fs);
		LifecycleHelper.initializeIfPossible(manager);
		LifecycleHelper.startIfPossible(manager);
		try {
			ReplicationStore oldStore = manager.create();
			File oldDir = storeBaseDir(oldStore);
			ReplicationStore newStore = manager.create();
			File newDir = storeBaseDir(newStore);
			Assert.assertNotEquals(oldDir, newDir);

			currentMetaRef(manager).set(staleLatest(oldDir.getName()));
			LifecycleHelper.stopIfPossible(manager);
			manager.setReadOnly(true);
			Assert.assertNull("currentMeta must be cleared", currentMetaRef(manager).get());
			manager.setReadOnly(false);
			LifecycleHelper.startIfPossible(manager);

			Assert.assertEquals(newDir, storeBaseDir(manager.getCurrent()));
		} finally {
			LifecycleHelper.stopIfPossible(manager);
			LifecycleHelper.disposeIfPossible(manager);
			fs.shutdown();
		}
	}

	@Test
	public void testReadOnlyGuardsCreateRecordGcAndAllowsDestroy() throws Exception {
		AsyncFileSystem fs = createTestAsyncFileSystem();
		DefaultReplicationStoreManager manager = newManager(fs);
		LifecycleHelper.initializeIfPossible(manager);
		LifecycleHelper.startIfPossible(manager);
		try {
			DefaultReplicationStore store = (DefaultReplicationStore) manager.create();
			Assert.assertTrue(store.getBaseDir().exists());
			LifecycleHelper.stopIfPossible(manager);
			manager.setReadOnly(true);
			LifecycleHelper.startIfPossible(manager);
			Assert.assertTrue(manager.isReadOnly());

			assertReadOnlyStore(manager::create);
			assertReadOnlyStore(() -> manager.recordLatestStore("poison"));
			assertReadOnlyStore(manager::gc);

			manager.destroy();
			Assert.assertFalse(store.getBaseDir().exists());
		} finally {
			try {
				LifecycleHelper.stopIfPossible(manager);
			} catch (Throwable ignore) {
			}
			try {
				LifecycleHelper.disposeIfPossible(manager);
			} catch (Throwable ignore) {
			}
			fs.shutdown();
		}
	}

	@Test
	public void testCreateIfNotExistReturnsNullWithoutMkdirWhenNoLatest() throws Exception {
		AsyncFileSystem fs = spy(createTestAsyncFileSystem());
		DefaultReplicationStoreManager manager = newManager(fs);
		try {
			LifecycleHelper.initializeIfPossible(manager);
			File metaFile = new File(manager.getBaseDir(), "store_manager_meta.properties");
			Assert.assertTrue(metaFile.getParentFile().mkdirs());
			try (FileOutputStream out = new FileOutputStream(metaFile)) {
				new Properties().store(out, null);
			}

			manager.setReadOnly(true);
			LifecycleHelper.startIfPossible(manager);
			clearInvocations(fs);

			Assert.assertNull(manager.createIfNotExist());
			Assert.assertNull(manager.getOpenedStore());
			verify(fs, never()).mkdir(anyString(), anyBoolean());
			verify(fs, never()).open(anyString(), eq(AbstractStorageFile.OpenMode.WRITE),
					anyBoolean(), anyBoolean(), any());
			verify(fs, never()).open(anyString(), eq(AbstractStorageFile.OpenMode.READ_WRITE),
					anyBoolean(), anyBoolean(), any());
			verify(fs, never()).open(anyString(), anyString(), anyList(), eq(true), anyString());
			verify(fs, atLeastOnce()).open(contains("store_manager_meta.properties"),
					eq(AbstractStorageFile.OpenMode.READ), eq(false), eq(true), any());
		} finally {
			LifecycleHelper.stopIfPossible(manager);
			LifecycleHelper.disposeIfPossible(manager);
			fs.shutdown();
		}
	}

	@Test
	public void testGetOpenedStoreAndSkipGcWhenReadOnly() throws Exception {
		AsyncFileSystem fs = spy(createTestAsyncFileSystem());
		DefaultReplicationStoreManager manager = newManager(fs);
		try {
			Assert.assertNull(manager.getOpenedStore());
			LifecycleHelper.initializeIfPossible(manager);
			LifecycleHelper.startIfPossible(manager);
			ReplicationStore store = manager.create();
			Assert.assertSame(store, manager.getOpenedStore());
			clearInvocations(fs);
			Assert.assertSame(store, manager.getOpenedStore());
			verify(fs, never()).open(anyString(), any(), anyBoolean(), anyBoolean(), any());

			LifecycleHelper.stopIfPossible(manager);
			Assert.assertNull(manager.getOpenedStore());

			manager.setReadOnly(true);
			LifecycleHelper.startIfPossible(manager);
			sleep(2000);
			Assert.assertEquals(0, manager.getGcCount());
		} finally {
			LifecycleHelper.stopIfPossible(manager);
			LifecycleHelper.disposeIfPossible(manager);
			fs.shutdown();
		}
	}

	private DefaultReplicationStoreManager newManager(AsyncFileSystem fs) {
		return new DefaultReplicationStoreManager(
				keeperConfig, getReplId(), randomKeeperRunid(), new File(getTestFileDir()),
				createkeeperMonitor(), mock(SyncRateManager.class), createRedisOpParser(), null, fs);
	}

	private static void assertReadOnlyStore(ThrowingRunnable action) throws Exception {
		try {
			action.run();
			Assert.fail("expected IllegalStateException(read only store)");
		} catch (IllegalStateException e) {
			Assert.assertEquals(DefaultReplicationStoreManager.READ_ONLY_STORE_MSG, e.getMessage());
		}
	}

	@SuppressWarnings("unchecked")
	private static AtomicReference<Properties> currentMetaRef(DefaultReplicationStoreManager manager) throws Exception {
		Field field = DefaultReplicationStoreManager.class.getDeclaredField("currentMeta");
		field.setAccessible(true);
		return (AtomicReference<Properties>) field.get(manager);
	}

	private static Properties staleLatest(String dirName) {
		Properties meta = new Properties();
		meta.setProperty("latest.store.dir", dirName);
		return meta;
	}

	private static File storeBaseDir(ReplicationStore store) {
		return new File(store.toString().substring("ReplicationStore:".length()));
	}

	@FunctionalInterface
	private interface ThrowingRunnable {
		void run() throws Exception;
	}
}

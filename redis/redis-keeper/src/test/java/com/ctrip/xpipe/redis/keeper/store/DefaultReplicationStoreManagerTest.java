package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofMarkType;
import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.core.redis.RunidGenerator;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.storage.AbstractStorageFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import java.util.concurrent.atomic.AtomicBoolean;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author marsqing
 *
 *         Jun 1, 2016 9:47:12 AM
 */
public class DefaultReplicationStoreManagerTest extends AbstractRedisKeeperTest {
	
	private int replicationStoreGcIntervalSeconds = 1;
	
	private int minTimeMilliToGcAfterCreate = 3000;
	
	private TestKeeperConfig keeperConfig;
	
	@Before
	public void beforeDefaultReplicationStoreManagerTest(){
		
		keeperConfig = new TestKeeperConfig();
		keeperConfig.setReplicationStoreGcIntervalSeconds(replicationStoreGcIntervalSeconds);
		keeperConfig.setMinTimeMilliToGcAfterCreate(minTimeMilliToGcAfterCreate);
	}

	@Test
	public void testNotCreateWhileNotInitialized() throws Exception {

		DefaultReplicationStoreManager replicationStoreManager = (DefaultReplicationStoreManager) createReplicationStoreManager(
				keeperConfig);
		try{
			replicationStoreManager.createIfNotExist();
			Assert.fail();
		}catch (Exception e){
			logger.warn(e.getMessage());
		}

		LifecycleHelper.initializeIfPossible(replicationStoreManager);
		replicationStoreManager.createIfNotExist();
		LifecycleHelper.startIfPossible(replicationStoreManager);
		replicationStoreManager.createIfNotExist();
		LifecycleHelper.stopIfPossible(replicationStoreManager);
		try {
			replicationStoreManager.createIfNotExist();
			Assert.fail("createIfNotExist should refuse after stop (PREPARE lease)");
		} catch (Exception e) {
			logger.warn(e.getMessage());
		}
		LifecycleHelper.disposeIfPossible(replicationStoreManager);

		logger.info("calling after dispose");
		try{
			replicationStoreManager.create();
			Assert.fail();
		}catch (Exception e){
			logger.warn(e.getMessage());
		}
	}

	/**
	 * T-R.11⑤: after Manager.stop() (PREPARE), {@code gc()} must skip list/rmdir and not reopen store.
	 */

	/**
	 * T-S.2 / T-S.3: closed store → checkOk false; createIfNotExist → create() self-heals
	 * (releaseCurrentStore inside create clears the previous lease).
	 */
	@Test
	public void testCreateIfNotExistHealsClosedStore() throws Exception {
		DefaultReplicationStoreManager manager = (DefaultReplicationStoreManager) createReplicationStoreManager(keeperConfig);
		LifecycleHelper.initializeIfPossible(manager);
		LifecycleHelper.startIfPossible(manager);
		try {
			ReplicationStore store = manager.createIfNotExist();
			Assert.assertTrue(store.checkOk());
			store.close();
			Assert.assertFalse(store.checkOk());

			ReplicationStore healed = manager.createIfNotExist();
			Assert.assertNotNull(healed);
			Assert.assertNotSame(store, healed);
			Assert.assertTrue(healed.checkOk());
			Assert.assertSame(healed, manager.getCurrent());
		} finally {
			LifecycleHelper.stopIfPossible(manager);
			LifecycleHelper.disposeIfPossible(manager);
		}
	}

	/**
	 * T-S.3 gate: after Manager.stop (PREPARE), bad/closed store must not self-heal via createIfNotExist.
	 */
	@Test
	public void testCreateIfNotExistRefusesHealWhenStopped() throws Exception {
		DefaultReplicationStoreManager manager = (DefaultReplicationStoreManager) createReplicationStoreManager(keeperConfig);
		LifecycleHelper.initializeIfPossible(manager);
		LifecycleHelper.startIfPossible(manager);
		try {
			ReplicationStore store = manager.createIfNotExist();
			store.close();
			LifecycleHelper.stopIfPossible(manager);
			Assert.assertTrue(manager.getLifecycleState().isPositivelyStopped());
			try {
				manager.createIfNotExist();
				Assert.fail("createIfNotExist must refuse after stop even when store is closed");
			} catch (IOException e) {
				logger.info("[testCreateIfNotExistRefusesHealWhenStopped] expected: {}", e.getMessage());
			}
			Assert.assertNull(manager.getCurrent());
		} finally {
			LifecycleHelper.disposeIfPossible(manager);
		}
	}

	/**
	 * §3.9.2 / Manager.create: saveMeta failure after construct keeps old store open and latest unchanged.
	 * Unpublished new store is closed. After T-S.5, inject write fail on long-lived meta handle.
	 */
	@Test
	public void testCreateFailureKeepsOldStoreOpen() throws Exception {
		AsyncFileSystem fs = spy(createTestAsyncFileSystem());
		File managerBase = new File(getTestFileDir());
		ReplId replId = getReplId();
		String keeperRunid = randomKeeperRunid();
		DefaultReplicationStoreManager manager = new DefaultReplicationStoreManager(
				keeperConfig, replId, keeperRunid, managerBase,
				createkeeperMonitor(), mock(SyncRateManager.class), createRedisOpParser(), null, fs);
		LifecycleHelper.initializeIfPossible(manager);
		LifecycleHelper.startIfPossible(manager);
		try {
			ReplicationStore oldStore = manager.create();
			Assert.assertTrue(oldStore.checkOk());
			Assert.assertSame(oldStore, manager.getCurrent());
			File oldStoreDir = storeBaseDir(oldStore);

			// After方案 B, construct writes meta.v2.json before manager meta — fail only manager-meta path.
			AtomicBoolean failManagerMetaWrite = new AtomicBoolean(false);
			doAnswer(invocation -> {
				if (failManagerMetaWrite.get()) {
					AsyncFile file = invocation.getArgument(0);
					if (isManagerMetaFile(file)) {
						return java.util.concurrent.CompletableFuture.failedFuture(
								new IOException("injected saveMeta write fail"));
					}
				}
				return invocation.callRealMethod();
			}).when(fs).write(any(AsyncFile.class), any(ByteBuf.class));

			failManagerMetaWrite.set(true);
			try {
				manager.create();
				Assert.fail("create should fail when saveMeta write fails");
			} catch (IOException e) {
				logger.info("[testCreateFailureKeepsOldStoreOpen] expected: {}", e.getMessage());
			} finally {
				failManagerMetaWrite.set(false);
			}

			Assert.assertSame(oldStore, manager.getCurrent());
			Assert.assertTrue(oldStore.checkOk());

			// latest.store.dir must still recover the old store after manager recycle.
			LifecycleHelper.stopIfPossible(manager);
			LifecycleHelper.disposeIfPossible(manager);
			DefaultReplicationStoreManager recovered = new DefaultReplicationStoreManager(
					keeperConfig, replId, keeperRunid, managerBase,
					createkeeperMonitor(), mock(SyncRateManager.class), createRedisOpParser(), null, fs);
			LifecycleHelper.initializeIfPossible(recovered);
			try {
				Assert.assertEquals(oldStoreDir, storeBaseDir(recovered.getCurrent()));
			} finally {
				LifecycleHelper.disposeIfPossible(recovered);
			}
		} finally {
			LifecycleHelper.stopIfPossible(manager);
			LifecycleHelper.disposeIfPossible(manager);
			try {
				fs.shutdown();
			} catch (Throwable ignore) {
			}
		}
	}

	/**
	 * §3.9.2 方案 B: construct fails before recordLatestStore → latest unchanged, old store still open.
	 */
	@Test
	public void testCreateConstructFailureKeepsLatestOnOldStore() throws Exception {
		AsyncFileSystem fs = createTestAsyncFileSystem();
		File managerBase = new File(getTestFileDir());
		ReplId replId = getReplId();
		String keeperRunid = randomKeeperRunid();
		AtomicBoolean failConstruct = new AtomicBoolean(false);
		DefaultReplicationStoreManager manager = new DefaultReplicationStoreManager(
				keeperConfig, replId, keeperRunid, managerBase,
				createkeeperMonitor(), mock(SyncRateManager.class), createRedisOpParser(), null, fs) {
			@Override
			protected ReplicationStore createReplicationStore(File storeBaseDir, KeeperConfig keeperConfig,
					String keeperRunid, KeeperMonitor keeperMonitor, SyncRateManager syncRateManager) throws IOException {
				if (failConstruct.get()) {
					throw new IOException("injected construct fail");
				}
				return super.createReplicationStore(storeBaseDir, keeperConfig, keeperRunid, keeperMonitor, syncRateManager);
			}
		};
		LifecycleHelper.initializeIfPossible(manager);
		LifecycleHelper.startIfPossible(manager);
		try {
			ReplicationStore oldStore = manager.create();
			File oldStoreDir = storeBaseDir(oldStore);
			Assert.assertSame(oldStore, manager.getCurrent());

			failConstruct.set(true);
			try {
				manager.create();
				Assert.fail("create should fail when construct fails");
			} catch (IOException e) {
				logger.info("[testCreateConstructFailureKeepsLatestOnOldStore] expected: {}", e.getMessage());
			} finally {
				failConstruct.set(false);
			}

			Assert.assertSame(oldStore, manager.getCurrent());
			Assert.assertTrue(oldStore.checkOk());

			LifecycleHelper.stopIfPossible(manager);
			LifecycleHelper.disposeIfPossible(manager);
			DefaultReplicationStoreManager recovered = new DefaultReplicationStoreManager(
					keeperConfig, replId, keeperRunid, managerBase,
					createkeeperMonitor(), mock(SyncRateManager.class), createRedisOpParser(), null, fs);
			LifecycleHelper.initializeIfPossible(recovered);
			try {
				Assert.assertEquals(oldStoreDir, storeBaseDir(recovered.getCurrent()));
			} finally {
				LifecycleHelper.disposeIfPossible(recovered);
			}
		} finally {
			LifecycleHelper.stopIfPossible(manager);
			LifecycleHelper.disposeIfPossible(manager);
			try {
				fs.shutdown();
			} catch (Throwable ignore) {
			}
		}
	}

	private static File storeBaseDir(ReplicationStore store) {
		return new File(store.toString().substring("ReplicationStore:".length()));
	}

	/** Manager meta path is package-private on {@link AsyncFile}; test scopes write-fail injection. */
	private static boolean isManagerMetaFile(AsyncFile file) {
		try {
			java.lang.reflect.Field pathField = AsyncFile.class.getDeclaredField("path");
			pathField.setAccessible(true);
			String path = (String) pathField.get(file);
			return path != null && path.endsWith("store_manager_meta.properties");
		} catch (ReflectiveOperationException e) {
			throw new IllegalStateException(e);
		}
	}

	@Test
	public void testGcSkippedWhenStoppedAfterPrepare() throws Exception {
		// Dedicated FS so verify(never) does not race with shared test FS.
		AsyncFileSystem fs = spy(createTestAsyncFileSystem());
		DefaultReplicationStoreManager manager = new DefaultReplicationStoreManager(
				keeperConfig, getReplId(), randomKeeperRunid(), new File(getTestFileDir()),
				createkeeperMonitor(), mock(SyncRateManager.class), createRedisOpParser(), null, fs);
		try {
			LifecycleHelper.initializeIfPossible(manager);
			LifecycleHelper.startIfPossible(manager);
			ReplicationStore store = manager.createIfNotExist();
			File storeDir = new File(store.toString().substring("ReplicationStore:".length()));
			Assert.assertTrue(storeDir.isDirectory());

			LifecycleHelper.stopIfPossible(manager);
			Assert.assertTrue(manager.getLifecycleState().isPositivelyStopped());
			Assert.assertNull(manager.getCurrent());

			clearInvocations(fs);
			manager.gc();
			verify(fs, never()).list(anyString());
			verify(fs, never()).rmdir(anyString(), anyBoolean());
			Assert.assertNull(manager.getCurrent());
			Assert.assertTrue(storeDir.isDirectory());
		} finally {
			LifecycleHelper.disposeIfPossible(manager);
			try {
				fs.shutdown();
			} catch (Throwable ignore) {
			}
		}
	}


	@Test
	public void testMultiManagerGc() throws Exception {
		
		String keeperRunid = RunidGenerator.DEFAULT.generateRunid();

		final DefaultReplicationStoreManager replicationStoreManager1 = (DefaultReplicationStoreManager) createReplicationStoreManager(keeperRunid, 
				keeperConfig);
		final DefaultReplicationStoreManager replicationStoreManager2 = (DefaultReplicationStoreManager) createReplicationStoreManager(keeperRunid,
				keeperConfig);

		LifecycleHelper.initializeIfPossible(replicationStoreManager1);
		LifecycleHelper.initializeIfPossible(replicationStoreManager2);
		// Phase Rb: gc() requires LifecycleState.isStarted(); schedule also moved to doStart.
		LifecycleHelper.startIfPossible(replicationStoreManager1);
		LifecycleHelper.startIfPossible(replicationStoreManager2);

		final AtomicReference<DefaultReplicationStore> store = new AtomicReference<DefaultReplicationStore>(null);

		for(int i = 0; i < 10; i++){
			
			logger.info("[testMultiManagerGc]{}", i);
			
			final CountDownLatch latch = new CountDownLatch(2);
			
			executors.execute(new Runnable() {
				
				@Override
				public void run() {
					
					try {
						store.set((DefaultReplicationStore) replicationStoreManager1.create());
					} catch (IOException e) {
						logger.error("[run]" + replicationStoreManager1, e);
					}finally{
						latch.countDown();
					}
				}
			});
			executors.execute(new Runnable() {
				
				@Override
				public void run() {

					try {
						replicationStoreManager2.gc();
					} catch (IOException e) {
						logger.error("[run]" + replicationStoreManager2, e);
					}finally{
						latch.countDown();
					}
				}
			});
			
			latch.await();
			Assert.assertNotNull(store.get());
			Assert.assertTrue(store.get().getBaseDir().exists());
		}
		
		sleep(minTimeMilliToGcAfterCreate + 1000);
		logger.info("[testMultiManagerGc][lastgc]");
		replicationStoreManager1.gc();
		File baseDir = replicationStoreManager1.getBaseDir();
		File []files = baseDir.listFiles();
		Assert.assertEquals(2, files.length);
	}
	
	
	@Test
	public void testCancelGc() throws Exception {

		DefaultReplicationStoreManager replicationStoreManager = (DefaultReplicationStoreManager) createReplicationStoreManager(
				keeperConfig);
		
		LifecycleHelper.initializeIfPossible(replicationStoreManager);
		LifecycleHelper.startIfPossible(replicationStoreManager);
		
		sleep(replicationStoreGcIntervalSeconds * 2000);
		

		LifecycleHelper.stopIfPossible(replicationStoreManager);
		LifecycleHelper.disposeIfPossible(replicationStoreManager);
		
		long gcCount = replicationStoreManager.getGcCount();
		Assert.assertTrue(gcCount > 0);

		sleep(replicationStoreGcIntervalSeconds * 2000);

		Assert.assertEquals(gcCount, replicationStoreManager.getGcCount());
	}

	
	@Test
	public void testDestroy() throws Exception{
		
		DefaultReplicationStoreManager replicationStoreManager = (DefaultReplicationStoreManager) createReplicationStoreManager(
				keeperConfig);
		
		LifecycleHelper.initializeIfPossible(replicationStoreManager);
		LifecycleHelper.startIfPossible(replicationStoreManager);
		
		DefaultReplicationStore store = (DefaultReplicationStore) replicationStoreManager.create();
		
		Assert.assertTrue(store.getBaseDir().exists());
		
		replicationStoreManager.destroy();
		
		Assert.assertTrue(!store.getBaseDir().exists());
		
	}

	@Test
	public void saveManagerMetaWithAtomicReplace() throws Exception {

		AsyncFileSystem fileSystem = spy(createTestAsyncFileSystem());
		DefaultReplicationStoreManager replicationStoreManager = new DefaultReplicationStoreManager(
				keeperConfig, getReplId(), randomKeeperRunid(), new File(getTestFileDir()), createkeeperMonitor(),
				mock(SyncRateManager.class), createRedisOpParser(), null, fileSystem);

		try {
			LifecycleHelper.initializeIfPossible(replicationStoreManager);
			replicationStoreManager.create();

			verify(fileSystem, atLeastOnce()).mkdir(contains(getReplId().toString()), eq(true));
			verify(fileSystem, atLeastOnce()).open(contains("store_manager_meta.properties"), eq(AbstractStorageFile.OpenMode.WRITE), eq(true), eq(true), eq(getReplId().toString()));
		} finally {
			LifecycleHelper.disposeIfPossible(replicationStoreManager);
			fileSystem.shutdown();
		}
	}
	
	
	@Test
	public void testConcurrentGc() throws Exception {

		final DefaultReplicationStoreManager mgr = (DefaultReplicationStoreManager) createReplicationStoreManager();

		LifecycleHelper.initializeIfPossible(mgr);
		// Phase Rb: gc() requires LifecycleState.isStarted().
		LifecycleHelper.startIfPossible(mgr);

		for (int i = 0; i < 10; i++) {

			logger.info("[testGc]{}", i);

			final CountDownLatch latch = new CountDownLatch(2);
			final AtomicReference<DefaultReplicationStore> store = new AtomicReference<DefaultReplicationStore>(null);

			executors.execute(new Runnable() {

				@Override
				public void run() {
					try {
						store.set((DefaultReplicationStore) mgr.create());
					} catch (IOException e) {
						logger.error("[testGc]", e);
					} finally {
						latch.countDown();
					}
				}
			});

			executors.execute(new Runnable() {

				@Override
				public void run() {

					try {
						mgr.gc();
					} catch (IOException e) {
						logger.error("[testGc][gc]", e);
					} finally {
						latch.countDown();
					}

				}
			});

			latch.await();
			Assert.assertNotNull(store.get());
			Assert.assertTrue(store.get().getBaseDir().exists());
		}
	}

	@Test
	public void test() throws Exception {
		
		String keeperRunid = randomKeeperRunid();
		
		File baseDir = new File(getTestFileDir());
		ReplId replId = getReplId();
		DefaultReplicationStoreManager mgr = (DefaultReplicationStoreManager) createReplicationStoreManager(replId, keeperRunid, baseDir);

		LifecycleHelper.initializeIfPossible(mgr);

		ReplicationStore currentStore = mgr.getCurrent();
		assertNull(currentStore);

		currentStore = mgr.create();

		assertEquals(replId, mgr.getReplId());
		assertEquals(currentStore, mgr.getCurrent());

		DefaultReplicationStore newCurrentStore = (DefaultReplicationStore) mgr.create();
		assertEquals(newCurrentStore, mgr.getCurrent());
		assertNotEquals(currentStore, mgr.getCurrent());

		MetaStore metaStore = newCurrentStore.getMetaStore();
		EofMarkType eofMarkType = new EofMarkType("12");
		metaStore.rdbConfirmPsync(metaStore.getReplId(), metaStore.beginOffset(),
				0, "", RdbStore.Type.NORMAL, eofMarkType, "");
		metaStore.setMasterAddress(new DefaultEndPoint("redis://127.0.0.1:6379"));
		RdbStore rdbStore = newCurrentStore.prepareRdb("masterRunid", 0, new LenEofType(100));
		rdbStore.updateRdbGtidSet(GtidSet.EMPTY_GTIDSET);
		rdbStore.updateRdbType(RdbStore.Type.NORMAL);
		newCurrentStore.confirmRdb(rdbStore);

		ByteBuf cmdBuf = Unpooled.buffer();
		cmdBuf.writeByte(9);
		newCurrentStore.cmdStore.appendCommands(cmdBuf);

		// Release long-lived meta writer before second manager recovers same store on shared FS.
		String expectedReplId = metaStore.getReplId();
		Long expectedBeginOffset = metaStore.beginOffset();
		DefaultEndPoint expectedMasterAddress = metaStore.getMasterAddress();
		LifecycleHelper.disposeIfPossible(mgr);

		DefaultReplicationStoreManager mgr2 = (DefaultReplicationStoreManager) createReplicationStoreManager(replId, keeperRunid, baseDir);
		LifecycleHelper.initializeIfPossible(mgr2);

		assertEquals(expectedReplId, mgr2.getCurrent().getMetaStore().getReplId());
		assertEquals(expectedBeginOffset, mgr2.getCurrent().getMetaStore().beginOffset());
		assertEquals(expectedMasterAddress, mgr2.getCurrent().getMetaStore().getMasterAddress());
		assertEquals(expectedBeginOffset, mgr2.getCurrent().getMetaStore().beginOffset());

		LifecycleHelper.disposeIfPossible(mgr2);
	}

}

package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserFactory;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.GeneralRedisOpParser;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.SERVER_TYPE;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.monitor.MasterStats;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import org.junit.Ignore;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.stream.IntStream;

import static com.ctrip.xpipe.redis.core.store.MetaStore.META_V2_FILE;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class DefaultReplicationStoreTest extends AbstractRedisKeeperTest{

	private File baseDir;
	
	private DefaultReplicationStore store;

	private RedisOpParser redisOpParser;

	@Before
	public void beforeDefaultReplicationStoreTest() throws IOException{
		RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
		RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
		redisOpParser = new GeneralRedisOpParser(redisOpParserManager);
		baseDir = new File(getTestFileDir());
	}

	private RdbStore beginRdb(ReplicationStore replicationStore, int dataLen) throws IOException {
		RdbStore rdbStore = replicationStore.prepareRdb(randomKeeperRunid(), -1, new LenEofType(dataLen));
		rdbStore.updateRdbGtidSet(GtidSet.EMPTY_GTIDSET);
		rdbStore.updateRdbType(RdbStore.Type.NORMAL);
		replicationStore.confirmRdb(rdbStore);
		return rdbStore;
	}

	/**
	 * T-H2.A1: confirmRdb meta save failure → no cmd / no rdb ref, I1, still fresh.
	 */
	@Test
	public void confirmRdbMetaWriteFailureKeepsI1() throws Exception {
		AsyncFileSystem fileSystem = spy(createTestAsyncFileSystem());
		AtomicBoolean failMetaWrite = new AtomicBoolean(false);
		doAnswer(invocation -> {
			AsyncFile file = invocation.getArgument(0);
			String path = (String) ReflectionTestUtils.getField(file, "path");
			if (failMetaWrite.get() && path != null && path.contains(META_V2_FILE)) {
				ByteBuf buf = invocation.getArgument(1);
				if (buf != null && buf.refCnt() > 0) {
					buf.release();
				}
				return java.util.concurrent.CompletableFuture.failedFuture(
						new IOException("injected confirmRdb meta write fail"));
			}
			return invocation.callRealMethod();
		}).when(fileSystem).write(any(AsyncFile.class), any(ByteBuf.class));

		try {
			store = new DefaultReplicationStore(baseDir, new DefaultKeeperConfig(), randomKeeperRunid(), createkeeperMonitor(),
					Mockito.mock(SyncRateManager.class), redisOpParser, fileSystem, getReplId());
			store.getMetaStore().becomeActive();

			RdbStore rdbStore = store.prepareRdb(randomKeeperRunid(), -1, new LenEofType(100));
			rdbStore.updateRdbGtidSet(GtidSet.EMPTY_GTIDSET);
			rdbStore.updateRdbType(RdbStore.Type.NORMAL);

			failMetaWrite.set(true);
			try {
				store.confirmRdb(rdbStore);
				Assert.fail("expected IOException when meta save fails");
			} catch (IOException expected) {
				Assert.assertTrue(expected.getMessage().contains("injected confirmRdb meta write fail")
						|| (expected.getCause() != null && expected.getCause().getMessage() != null
						&& expected.getCause().getMessage().contains("injected confirmRdb meta write fail")));
			} finally {
				failMetaWrite.set(false);
			}

			Assert.assertNull(store.getRdbStore());
			Assert.assertNull(ReflectionTestUtils.getField(store, "cmdStore"));
			Assert.assertNull(store.getMetaStore().dupReplicationStoreMeta().getCmdFilePrefix());
			Assert.assertTrue(store.isFresh());
		} finally {
			if (store != null) {
				try {
					store.close();
				} catch (Exception ignore) {
				}
				store = null;
			}
			fileSystem.shutdown();
		}
	}

	/**
	 * T-H2.A1: concurrent meta update between prepare and save → CAS fail, I1, concurrent update kept.
	 */
	@Test
	public void confirmRdbMetaCasFailureKeepsI1() throws Exception {
		store = new DefaultReplicationStore(baseDir, new DefaultKeeperConfig(), randomKeeperRunid(), createkeeperMonitor(),
				Mockito.mock(SyncRateManager.class), redisOpParser, asyncFileSystem(), getReplId()) {
			@Override
			protected CommandStore createCommandStore(File baseDir, ReplicationStoreMeta replMeta, int cmdFileSize,
													  KeeperConfig config, CommandReaderWriterFactory cmdReaderWriterFactory,
													  KeeperMonitor keeperMonitor, GtidCmdFilter gtidCmdFilter) throws IOException {
				getMetaStore().setRdbFileSize(9999);
				return super.createCommandStore(baseDir, replMeta, cmdFileSize, config, cmdReaderWriterFactory,
						keeperMonitor, gtidCmdFilter);
			}
		};
		store.getMetaStore().becomeActive();

		RdbStore rdbStore = store.prepareRdb(randomKeeperRunid(), -1, new LenEofType(100));
		rdbStore.updateRdbGtidSet(GtidSet.EMPTY_GTIDSET);
		rdbStore.updateRdbType(RdbStore.Type.NORMAL);

		try {
			store.confirmRdb(rdbStore);
			Assert.fail("expected IOException when meta CAS fails");
		} catch (IOException expected) {
			Assert.assertTrue(expected.getMessage().contains("meta CAS fail"));
		}

		Assert.assertNull(store.getRdbStore());
		Assert.assertNull(ReflectionTestUtils.getField(store, "cmdStore"));
		Assert.assertNull(store.getMetaStore().dupReplicationStoreMeta().getCmdFilePrefix());
		Assert.assertEquals(9999L, store.getMetaStore().dupReplicationStoreMeta().getRdbFileSize());
		Assert.assertTrue(store.isFresh());
	}

	/**
	 * T-H2.D1: checkReplIdAndUpdateRdb meta write fail → storeRef keeps old RDB, meta rdb file unchanged.
	 */
	@Test
	public void checkReplIdAndUpdateRdbMetaWriteFailureKeepsOldRdb() throws Exception {
		AsyncFileSystem fileSystem = spy(createTestAsyncFileSystem());
		AtomicBoolean failMetaWrite = new AtomicBoolean(false);
		doAnswer(invocation -> {
			AsyncFile file = invocation.getArgument(0);
			String path = (String) ReflectionTestUtils.getField(file, "path");
			if (failMetaWrite.get() && path != null && path.contains(META_V2_FILE)) {
				ByteBuf buf = invocation.getArgument(1);
				if (buf != null && buf.refCnt() > 0) {
					buf.release();
				}
				return java.util.concurrent.CompletableFuture.failedFuture(
						new IOException("injected updateRdb meta write fail"));
			}
			return invocation.callRealMethod();
		}).when(fileSystem).write(any(AsyncFile.class), any(ByteBuf.class));

		try {
			store = new DefaultReplicationStore(baseDir, new DefaultKeeperConfig(), randomKeeperRunid(), createkeeperMonitor(),
					Mockito.mock(SyncRateManager.class), redisOpParser, fileSystem, getReplId());
			store.getMetaStore().becomeActive();

			String replId = randomKeeperRunid();
			RdbStore rdb1 = store.prepareRdb(replId, -1, new LenEofType(100));
			rdb1.updateRdbGtidSet(GtidSet.EMPTY_GTIDSET);
			rdb1.updateRdbType(RdbStore.Type.NORMAL);
			store.confirmRdb(rdb1);

			RdbStore oldRdbStore = store.getRdbStore();
			String oldRdbFile = store.getMetaStore().dupReplicationStoreMeta().getRdbFile();
			Assert.assertNotNull(oldRdbStore);
			Assert.assertNotNull(oldRdbFile);

			RdbStore rdb2 = store.prepareRdb(replId, 100, new LenEofType(100));
			rdb2.updateRdbGtidSet(GtidSet.EMPTY_GTIDSET);
			rdb2.updateRdbType(RdbStore.Type.NORMAL);

			failMetaWrite.set(true);
			try {
				store.checkReplIdAndUpdateRdb(rdb2);
				Assert.fail("expected IOException when meta save fails");
			} catch (IOException expected) {
				Assert.assertTrue(expected.getMessage().contains("injected updateRdb meta write fail")
						|| (expected.getCause() != null && expected.getCause().getMessage() != null
						&& expected.getCause().getMessage().contains("injected updateRdb meta write fail")));
			} finally {
				failMetaWrite.set(false);
			}

			// storeRef must still point to the old RDB; meta rdb file unchanged.
			Assert.assertSame(oldRdbStore, store.getRdbStore());
			Assert.assertEquals(oldRdbFile, store.getMetaStore().dupReplicationStoreMeta().getRdbFile());
		} finally {
			if (store != null) {
				try {
					store.close();
				} catch (Exception ignore) {
				}
				store = null;
			}
			fileSystem.shutdown();
		}
	}

	/**
	 * T-H2.A2: continueFromOffset createCmd ok + meta save fail → new cmd closed/not exposed; prefix unchanged.
	 */
	@Test
	public void continueFromOffsetMetaWriteFailureDoesNotExposeNewCmd() throws Exception {
		AsyncFileSystem fileSystem = spy(createTestAsyncFileSystem());
		AtomicBoolean failMetaWrite = new AtomicBoolean(false);
		doAnswer(invocation -> {
			AsyncFile file = invocation.getArgument(0);
			String path = (String) ReflectionTestUtils.getField(file, "path");
			if (failMetaWrite.get() && path != null && path.contains(META_V2_FILE)) {
				ByteBuf buf = invocation.getArgument(1);
				if (buf != null && buf.refCnt() > 0) {
					buf.release();
				}
				return java.util.concurrent.CompletableFuture.failedFuture(
						new IOException("injected continueFromOffset meta write fail"));
			}
			return invocation.callRealMethod();
		}).when(fileSystem).write(any(AsyncFile.class), any(ByteBuf.class));

		try {
			store = new DefaultReplicationStore(baseDir, new DefaultKeeperConfig(), randomKeeperRunid(), createkeeperMonitor(),
					Mockito.mock(SyncRateManager.class), redisOpParser, fileSystem, getReplId());
			store.getMetaStore().becomeActive();

			String replId = randomKeeperRunid();
			failMetaWrite.set(true);
			try {
				store.continueFromOffset(replId, 1L);
				Assert.fail("expected IOException when meta save fails");
			} catch (IOException expected) {
				Assert.assertTrue(expected.getMessage().contains("injected continueFromOffset meta write fail")
						|| (expected.getCause() != null && expected.getCause().getMessage() != null
						&& expected.getCause().getMessage().contains("injected continueFromOffset meta write fail")));
			} finally {
				failMetaWrite.set(false);
			}

			Assert.assertNull(ReflectionTestUtils.getField(store, "cmdStore"));
			Assert.assertNull(store.getMetaStore().dupReplicationStoreMeta().getCmdFilePrefix());
			Assert.assertTrue(store.isFresh());
		} finally {
			if (store != null) {
				try {
					store.close();
				} catch (Exception ignore) {
				}
				store = null;
			}
			fileSystem.shutdown();
		}
	}

	/**
	 * T-H2.A2: psyncContinueFrom createCmd ok + meta save fail → new cmd not exposed; prior cmd/prefix kept.
	 */
	@Test
	public void psyncContinueFromMetaWriteFailureKeepsOldCmd() throws Exception {
		AsyncFileSystem fileSystem = spy(createTestAsyncFileSystem());
		AtomicBoolean failMetaWrite = new AtomicBoolean(false);
		doAnswer(invocation -> {
			AsyncFile file = invocation.getArgument(0);
			String path = (String) ReflectionTestUtils.getField(file, "path");
			if (failMetaWrite.get() && path != null && path.contains(META_V2_FILE)) {
				ByteBuf buf = invocation.getArgument(1);
				if (buf != null && buf.refCnt() > 0) {
					buf.release();
				}
				return java.util.concurrent.CompletableFuture.failedFuture(
						new IOException("injected psyncContinueFrom meta write fail"));
			}
			return invocation.callRealMethod();
		}).when(fileSystem).write(any(AsyncFile.class), any(ByteBuf.class));

		try {
			store = new DefaultReplicationStore(baseDir, new DefaultKeeperConfig(), randomKeeperRunid(), createkeeperMonitor(),
					Mockito.mock(SyncRateManager.class), redisOpParser, fileSystem, getReplId());
			store.getMetaStore().becomeActive();

			RdbStore rdbStore = beginRdb(store, 100);
			rdbStore.writeRdb(Unpooled.wrappedBuffer(randomString(100).getBytes()));
			rdbStore.endRdb();

			Object oldCmdStore = ReflectionTestUtils.getField(store, "cmdStore");
			String oldPrefix = store.getMetaStore().dupReplicationStoreMeta().getCmdFilePrefix();
			Assert.assertNotNull(oldCmdStore);
			Assert.assertNotNull(oldPrefix);

			failMetaWrite.set(true);
			try {
				store.psyncContinueFrom("repl_continue", 1L);
				Assert.fail("expected IOException when meta save fails");
			} catch (IOException expected) {
				Assert.assertTrue(expected.getMessage().contains("injected psyncContinueFrom meta write fail")
						|| (expected.getCause() != null && expected.getCause().getMessage() != null
						&& expected.getCause().getMessage().contains("injected psyncContinueFrom meta write fail")));
			} finally {
				failMetaWrite.set(false);
			}

			Assert.assertSame(oldCmdStore, ReflectionTestUtils.getField(store, "cmdStore"));
			Assert.assertEquals(oldPrefix, store.getMetaStore().dupReplicationStoreMeta().getCmdFilePrefix());
		} finally {
			if (store != null) {
				try {
					store.close();
				} catch (Exception ignore) {
				}
				store = null;
			}
			fileSystem.shutdown();
		}
	}

	/**
	 * T-H2.A1: confirmRdb createCommandStore failure → meta not committed, no rdb ref.
	 */
	@Test
	public void confirmRdbCreateCmdFailureKeepsI1() throws Exception {
		store = new DefaultReplicationStore(baseDir, new DefaultKeeperConfig(), randomKeeperRunid(), createkeeperMonitor(),
				Mockito.mock(SyncRateManager.class), redisOpParser, asyncFileSystem(), getReplId()) {
			@Override
			protected CommandStore createCommandStore(File baseDir, ReplicationStoreMeta replMeta, int cmdFileSize,
													  KeeperConfig config, CommandReaderWriterFactory cmdReaderWriterFactory,
													  KeeperMonitor keeperMonitor, GtidCmdFilter gtidCmdFilter) throws IOException {
				throw new IOException("injected createCommandStore fail");
			}
		};
		store.getMetaStore().becomeActive();

		RdbStore rdbStore = store.prepareRdb(randomKeeperRunid(), -1, new LenEofType(100));
		rdbStore.updateRdbGtidSet(GtidSet.EMPTY_GTIDSET);
		rdbStore.updateRdbType(RdbStore.Type.NORMAL);

		try {
			store.confirmRdb(rdbStore);
			Assert.fail("expected IOException when createCommandStore fails");
		} catch (IOException expected) {
			Assert.assertTrue(expected.getMessage().contains("injected createCommandStore fail"));
		}

		Assert.assertNull(store.getRdbStore());
		Assert.assertNull(ReflectionTestUtils.getField(store, "cmdStore"));
		Assert.assertNull(store.getMetaStore().dupReplicationStoreMeta().getCmdFilePrefix());
		Assert.assertTrue(store.isFresh());
	}

	@Test
	public void testReadWhileDestroy() throws Exception{

		store = new DefaultReplicationStore(baseDir, new DefaultKeeperConfig(), randomKeeperRunid(), createkeeperMonitor(), Mockito.mock(SyncRateManager.class), redisOpParser, asyncFileSystem(), getReplId());
		store.getMetaStore().becomeActive();

		int dataLen = 1000;
		RdbStore rdbStore = beginRdb(store, dataLen);
		
		rdbStore.writeRdb(Unpooled.wrappedBuffer(randomString(dataLen).getBytes()));
		rdbStore.endRdb();
		
		CountDownLatch latch  = new CountDownLatch(2);
		AtomicBoolean result = new AtomicBoolean(true);
		
		executors.execute(new AbstractExceptionLogTask() {
			
			@Override
			protected void doRun() throws Exception {
				
				try{
					sleep(2);
					store.close();
					store.destroy();
				}finally{
					latch.countDown();
				}
			}
		});
		
	
		executors.execute(new AbstractExceptionLogTask() {
			
			@Override
			protected void doRun() throws Exception {
				
				try{
					store.fullSyncIfPossible(new FullSyncListener() {

						@Override
						public boolean supportRdb(RdbStore.Type rdbType) {
							return true;
						}

						@Override
						public ChannelFuture onCommand(Object cmd) {
							
							return null;
						}

						@Override
						public void onCommandEnd() {

						}

						@Override
						public void beforeCommand() {
							
						}

						@Override
						public void setRdbFileInfo(EofType eofType, ReplicationProgress<?> rdbProgress) {

						}

						@Override
						public boolean supportProgress(Class<? extends ReplicationProgress<?>> clazz) {
							return true;
						}

						@Override
						public void onFileData(ReferenceFileRegion referenceFileRegion) throws IOException {
							sleep(100);
						}
						
						@Override
						public boolean isOpen() {
							return true;
						}
						
						@Override
						public void exception(Exception e) {
							logger.info("[exception][fail]" + e.getMessage());
							result.set(false);
						}
						
						@Override
						public void beforeFileData() {
							
						}

						@Override
						public Long processedBacklogOffset() {
							return null;
						}
					});
				}catch(Exception e){
					logger.info("[exception][fail]" + e.getMessage());
					result.set(false);
				}finally{
					latch.countDown();
				}
			}
		});
		
		
		Assert.assertTrue(latch.await(2000, TimeUnit.MILLISECONDS));
		Assert.assertFalse(result.get());
	}

	
	@Test
	@Ignore("FS bug: cmd segment write path throws FileAlreadyExistsException — Phase FS T-FS.1/T-FS.3")
	public void testReadWrite() throws Exception {

		store = new DefaultReplicationStore(baseDir, new DefaultKeeperConfig(), randomKeeperRunid(), createkeeperMonitor(), Mockito.mock(SyncRateManager.class), redisOpParser, asyncFileSystem(), getReplId());
		store.getMetaStore().becomeActive();


		StringBuffer exp = new StringBuffer();

		int cmdCount = 4;
		int cmdLen = 10;

		beginRdb(store, -1);

		for (int j = 0; j < cmdCount; j++) {
			ByteBuf buf = Unpooled.buffer();
			String cmd = UUID.randomUUID().toString().substring(0, cmdLen);
			exp.append(cmd);
			buf.writeBytes(cmd.getBytes());
			store.cmdStore.appendCommands(buf);
		}
		String result = readCommandFileTilEnd(store, exp.length());
		assertEquals(exp.toString(), result);
		store.close();
	}

	@Test
	@Ignore("FS bug: cmd appendCommands fails (FileAlreadyExistsException), so second gc cannot clear rdb — Phase FS T-FS.1/T-FS.3")
	public void testGcNotContinueRdb() throws Exception {
		TestKeeperConfig config = new TestKeeperConfig(100, 1, 1024, 0);
		store = new DefaultReplicationStore(baseDir, config, randomKeeperRunid(), createkeeperMonitor(), Mockito.mock(SyncRateManager.class), redisOpParser, asyncFileSystem(), getReplId());
		store.getMetaStore().becomeActive();

		int dataLen = 100;
		RdbStore rdbStore = beginRdb(store, dataLen);

		store.psyncContinueFrom("repl", 1);

		rdbStore.writeRdb(Unpooled.wrappedBuffer(randomString(dataLen).getBytes()));
		rdbStore.endRdb();

		IntStream.range(0,5).forEach(i -> {
			try {
				ReflectionTestUtils.setField(store.cmdStore, "buildIndex", false);
				store.cmdStore.appendCommands(Unpooled.wrappedBuffer(randomString(100).getBytes()));
			} catch (Exception e) {
				logger.info("[testGcNotContinueRdb][append cmd fail]", e);
			}
		});

		store.gc(); // just release cmd files
		Assert.assertNotNull(store.getRdbStore());

		store.gc();
		Assert.assertNull(store.getRdbStore());
		Assert.assertNull(store.getMetaStore().dupReplicationStoreMeta().getRdbFile());
	}

	@Test
	public void testCmdNotifyCoalescingEnabledOnlyForRedisUpstream() throws Exception {
		KeeperMonitor keeperMonitor = Mockito.mock(KeeperMonitor.class);
		MasterStats masterStats = Mockito.mock(MasterStats.class);
		Mockito.when(keeperMonitor.getMasterStats()).thenReturn(masterStats);
		Mockito.when(masterStats.currentMasterType()).thenReturn(SERVER_TYPE.REDIS);

		TestKeeperConfig config = new TestKeeperConfig() {
			@Override
			public boolean isCommandOffsetNotifyCoalescingEnabled() {
				return true;
			}
		};
		store = new DefaultReplicationStore(baseDir, config, randomKeeperRunid(), keeperMonitor,
				Mockito.mock(SyncRateManager.class), redisOpParser, asyncFileSystem(), getReplId());

		Assert.assertTrue((Boolean) ReflectionTestUtils.invokeMethod(store, "isCmdNotifyCoalescingEnabled"));
		Mockito.when(masterStats.currentMasterType()).thenReturn(SERVER_TYPE.KEEPER);
		Assert.assertFalse((Boolean) ReflectionTestUtils.invokeMethod(store, "isCmdNotifyCoalescingEnabled"));
	}

	@Test
	public void testCmdNotifyCoalescingDisabledByConfig() throws Exception {
		KeeperMonitor keeperMonitor = Mockito.mock(KeeperMonitor.class);
		MasterStats masterStats = Mockito.mock(MasterStats.class);
		Mockito.when(keeperMonitor.getMasterStats()).thenReturn(masterStats);
		Mockito.when(masterStats.currentMasterType()).thenReturn(SERVER_TYPE.REDIS);

		TestKeeperConfig config = new TestKeeperConfig() {
			@Override
			public boolean isCommandOffsetNotifyCoalescingEnabled() {
				return false;
			}
		};
		store = new DefaultReplicationStore(baseDir, config, randomKeeperRunid(), keeperMonitor,
				Mockito.mock(SyncRateManager.class), redisOpParser, asyncFileSystem(), getReplId());

		Assert.assertFalse((Boolean) ReflectionTestUtils.invokeMethod(store, "isCmdNotifyCoalescingEnabled"));
	}

	@Test
	public void testCreateCommandStoreUsesDynamicCoalescingSupplier() throws Exception {
		KeeperMonitor keeperMonitor = Mockito.mock(KeeperMonitor.class);
		MasterStats masterStats = Mockito.mock(MasterStats.class);
		Mockito.when(keeperMonitor.getMasterStats()).thenReturn(masterStats);
		Mockito.when(masterStats.currentMasterType()).thenReturn(SERVER_TYPE.REDIS);

		TestKeeperConfig config = new TestKeeperConfig() {
			@Override
			public boolean isCommandOffsetNotifyCoalescingEnabled() {
				return true;
			}
		};
		store = new DefaultReplicationStore(baseDir, config, randomKeeperRunid(), keeperMonitor,
				Mockito.mock(SyncRateManager.class), redisOpParser, asyncFileSystem(), getReplId());
		store.getMetaStore().becomeActive();
		beginRdb(store, 1);

		BooleanSupplier coalescingEnabled = (BooleanSupplier) ReflectionTestUtils.getField(
				store.cmdStore, "commandOffsetNotifyCoalescingEnabled");
		Assert.assertNotNull(coalescingEnabled);
		Assert.assertTrue(coalescingEnabled.getAsBoolean());

		Mockito.when(masterStats.currentMasterType()).thenReturn(SERVER_TYPE.KEEPER);
		Assert.assertFalse(coalescingEnabled.getAsBoolean());
	}

	@Test
	public void testCmdNotifyCoalescingDisabledForUnknownUpstream() throws Exception {
		KeeperMonitor keeperMonitor = Mockito.mock(KeeperMonitor.class);
		MasterStats masterStats = Mockito.mock(MasterStats.class);
		Mockito.when(keeperMonitor.getMasterStats()).thenReturn(masterStats);
		Mockito.when(masterStats.currentMasterType()).thenReturn(SERVER_TYPE.UNKNOWN);

		TestKeeperConfig config = new TestKeeperConfig() {
			@Override
			public boolean isCommandOffsetNotifyCoalescingEnabled() {
				return true;
			}
		};
		store = new DefaultReplicationStore(baseDir, config, randomKeeperRunid(), keeperMonitor,
				Mockito.mock(SyncRateManager.class), redisOpParser, asyncFileSystem(), getReplId());

		Assert.assertFalse((Boolean) ReflectionTestUtils.invokeMethod(store, "isCmdNotifyCoalescingEnabled"));
	}

	@Test
	public void testCreateCommandStoreSupplierReflectsConfigToggle() throws Exception {
		KeeperMonitor keeperMonitor = Mockito.mock(KeeperMonitor.class);
		MasterStats masterStats = Mockito.mock(MasterStats.class);
		Mockito.when(keeperMonitor.getMasterStats()).thenReturn(masterStats);
		Mockito.when(masterStats.currentMasterType()).thenReturn(SERVER_TYPE.REDIS);

		AtomicReference<Boolean> enabled = new AtomicReference<>(true);
		TestKeeperConfig config = new TestKeeperConfig() {
			@Override
			public boolean isCommandOffsetNotifyCoalescingEnabled() {
				return enabled.get();
			}
		};
		store = new DefaultReplicationStore(baseDir, config, randomKeeperRunid(), keeperMonitor,
				Mockito.mock(SyncRateManager.class), redisOpParser, asyncFileSystem(), getReplId());
		store.getMetaStore().becomeActive();
		beginRdb(store, 1);

		BooleanSupplier coalescingEnabled = (BooleanSupplier) ReflectionTestUtils.getField(
				store.cmdStore, "commandOffsetNotifyCoalescingEnabled");
		Assert.assertTrue(coalescingEnabled.getAsBoolean());
		enabled.set(false);
		Assert.assertFalse(coalescingEnabled.getAsBoolean());
	}

}

package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.filechannel.DefaultReferenceFileRegion;
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
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.monitor.MasterStats;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
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

import static org.junit.Assert.assertEquals;

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

	@Test
	public void testReadWhileDestroy() throws Exception{

		store = new DefaultReplicationStore(baseDir, new DefaultKeeperConfig(), randomKeeperRunid(), createkeeperMonitor(), Mockito.mock(SyncRateManager.class), redisOpParser);
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
						public ChannelFuture onCommand(CommandFile currentFile, long filePosition, Object cmd) {
							
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
						public void onFileData(DefaultReferenceFileRegion referenceFileRegion) throws IOException {
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
	public void testReadWrite() throws Exception {

		store = new DefaultReplicationStore(baseDir, new DefaultKeeperConfig(), randomKeeperRunid(), createkeeperMonitor(), Mockito.mock(SyncRateManager.class), redisOpParser);
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
	public void testGcNotContinueRdb() throws Exception {
		TestKeeperConfig config = new TestKeeperConfig(100, 1, 1024, 0);
		store = new DefaultReplicationStore(baseDir, config, randomKeeperRunid(), createkeeperMonitor(), Mockito.mock(SyncRateManager.class), redisOpParser);
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
				Mockito.mock(SyncRateManager.class), redisOpParser);

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
				Mockito.mock(SyncRateManager.class), redisOpParser);

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
				Mockito.mock(SyncRateManager.class), redisOpParser);
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
				Mockito.mock(SyncRateManager.class), redisOpParser);

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
				Mockito.mock(SyncRateManager.class), redisOpParser);
		store.getMetaStore().becomeActive();
		beginRdb(store, 1);

		BooleanSupplier coalescingEnabled = (BooleanSupplier) ReflectionTestUtils.getField(
				store.cmdStore, "commandOffsetNotifyCoalescingEnabled");
		Assert.assertTrue(coalescingEnabled.getAsBoolean());
		enabled.set(false);
		Assert.assertFalse(coalescingEnabled.getAsBoolean());
	}

}

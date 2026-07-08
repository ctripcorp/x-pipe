package com.ctrip.xpipe.redis.keeper;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.payload.ByteArrayWritableByteChannel;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.redis.RunidGenerator;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserFactory;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.GeneralRedisOpParser;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.monitor.KeepersMonitorManager;
import com.ctrip.xpipe.redis.keeper.monitor.impl.NoneKeepersMonitorManager;
import com.ctrip.xpipe.redis.keeper.monitor.impl.NoneKeepersMonitorManager.NoneKeeperMonitor;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncTFSBasedFileSystem;
import com.ctrip.xpipe.redis.keeper.store.AbstractCommandStore;
import com.ctrip.xpipe.redis.keeper.store.DefaultCommandStore;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.store.AsyncCommandStore;
import com.ctrip.xpipe.redis.keeper.store.cmd.OffsetCommandReaderWriterFactory;
import com.ctrip.xpipe.redis.keeper.store.ck.CKStore;
import com.ctrip.xpipe.redis.core.store.OffsetReplicationProgress;
import io.netty.channel.ChannelFuture;
import org.junit.After;
import org.junit.BeforeClass;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;

/**
 * @author wenchao.meng
 *
 *         Jun 12, 2016
 */
public class AbstractRedisKeeperTest extends AbstractRedisTest {
	@BeforeClass
	public static void beforeAbstractCheckerTest(){
		System.setProperty("DisableLoadProxyAgentJar", "true");
	}

	private AsyncFileSystem testAsyncFileSystem;

	protected AsyncFileSystem asyncFileSystem() {
		if (testAsyncFileSystem == null) {
			testAsyncFileSystem = new AsyncTFSBasedFileSystem(1, KeeperConfig.DEFAULT_ASYNC_FSYNC_INTERVAL_BYTES);
		}
		return testAsyncFileSystem;
	}

	@After
	public void shutdownTestAsyncFileSystem() {
		if (testAsyncFileSystem != null) {
			testAsyncFileSystem.shutdown();
			testAsyncFileSystem = null;
		}
	}

	protected ClusterId getClusterId() {
		return new ClusterId(currentTestName()  + "-", 0L);
	}

	protected ClusterId getClusterId(Long id) {
		return new ClusterId(currentTestName()  + "-", id);
	}

	protected ReplId getReplId() {
		return new ReplId(currentTestName() + "-", 0L);
	}

	protected DefaultCommandStore createDefaultCommandStore(File file, int maxFileSize,
			CommandReaderWriterFactory cmdReaderWriterFactory, KeeperMonitor keeperMonitor,
			RedisOpParser redisOpParser, GtidCmdFilter gtidCmdFilter) throws IOException {
		return createDefaultCommandStore(null, getKeeperConfig(), file, maxFileSize, () -> false, () -> 12 * 3600,
				3600 * 1000, () -> 20, AbstractCommandStore.DEFAULT_COMMAND_READER_FLYING_THRESHOLD, () -> true,
				cmdReaderWriterFactory, keeperMonitor, redisOpParser, gtidCmdFilter, true);
	}

	protected DefaultCommandStore createDefaultCommandStore(CKStore ckStore, KeeperConfig keeperConfig, File file,
			int maxFileSize, BooleanSupplier recordWrongStreamConfig,
			IntSupplier maxTimeSecondKeeperCmdFileAfterModified, int minTimeMilliToGcAfterModified,
			IntSupplier fileNumToKeep, long commandReaderFlyingThreshold,
			BooleanSupplier commandOffsetNotifyCoalescingEnabled,
			CommandReaderWriterFactory cmdReaderWriterFactory, KeeperMonitor keeperMonitor,
			RedisOpParser redisOpParser, GtidCmdFilter gtidCmdFilter, boolean buildIndex) throws IOException {
		return new DefaultCommandStore(ckStore, keeperConfig, file, maxFileSize, recordWrongStreamConfig,
				maxTimeSecondKeeperCmdFileAfterModified, minTimeMilliToGcAfterModified, fileNumToKeep,
				commandReaderFlyingThreshold, commandOffsetNotifyCoalescingEnabled, cmdReaderWriterFactory,
				keeperMonitor, redisOpParser, gtidCmdFilter, buildIndex, asyncFileSystem(),
				() -> AsyncCommandStore.DEFAULT_ASYNC_WRITE_MAX_BYTES, getReplId());
	}

	protected DefaultCommandStore createDefaultCommandStore(CKStore ckStore, KeeperConfig keeperConfig, File file,
			int maxFileSize, BooleanSupplier recordWrongStreamConfig,
			IntSupplier maxTimeSecondKeeperCmdFileAfterModified, int minTimeMilliToGcAfterModified,
			IntSupplier fileNumToKeep, long commandReaderFlyingThreshold,
			BooleanSupplier commandOffsetNotifyCoalescingEnabled,
			CommandReaderWriterFactory cmdReaderWriterFactory, KeeperMonitor keeperMonitor,
			RedisOpParser redisOpParser, GtidCmdFilter gtidCmdFilter, boolean buildIndex,
			IntSupplier asyncWriteMaxBytes) throws IOException {
		return new DefaultCommandStore(ckStore, keeperConfig, file, maxFileSize, recordWrongStreamConfig,
				maxTimeSecondKeeperCmdFileAfterModified, minTimeMilliToGcAfterModified, fileNumToKeep,
				commandReaderFlyingThreshold, commandOffsetNotifyCoalescingEnabled, cmdReaderWriterFactory,
				keeperMonitor, redisOpParser, gtidCmdFilter, buildIndex, asyncFileSystem(), asyncWriteMaxBytes, getReplId());
	}

	protected DefaultReplicationStore createDefaultReplicationStore(File baseDir, KeeperConfig config, String keeperRunid,
			KeeperMonitor keeperMonitor, SyncRateManager syncRateManager, RedisOpParser redisOpParser) throws IOException {
		return new DefaultReplicationStore(null, baseDir, config, keeperRunid, new OffsetCommandReaderWriterFactory(),
				keeperMonitor, syncRateManager, redisOpParser, null, asyncFileSystem(), getReplId());
	}

	protected ShardId getShardId() {
	    return new ShardId(currentTestName() + "-", 0L);
	}

	protected ShardId getShardId(Long id) {
		return new ShardId(currentTestName() + "-", id);
	}

	protected ReplicationStoreManager createReplicationStoreManager(String keeperRunid, KeeperConfig keeperConfig) {
		
		String tmpDir = getTestFileDir();

		return createReplicationStoreManager(getReplId(), keeperRunid, keeperConfig, new File(tmpDir));
	}

	protected ReplicationStoreManager createReplicationStoreManager(KeeperConfig keeperConfig) {
		
		String tmpDir = getTestFileDir();

		return createReplicationStoreManager(getReplId(), keeperConfig, new File(tmpDir));
	}

	
	protected ReplicationStoreManager createReplicationStoreManager() {

		String tmpDir = getTestFileDir();

		return createReplicationStoreManager(getReplId(), new File(tmpDir));
	}

	protected ReplicationStoreManager createReplicationStoreManager(ReplId replId, String keeperRunid, File storeDir) {

		return createReplicationStoreManager(replId, keeperRunid, getKeeperConfig(), storeDir);
	}

	protected ReplicationStoreManager createReplicationStoreManager(ReplId replId, File storeDir) {

		return createReplicationStoreManager(replId, getKeeperConfig(), storeDir);
	}
	
	protected KeeperConfig getKeeperConfig() {
		return new TestKeeperConfig();
	}

	protected ReplicationStoreManager createReplicationStoreManager(ReplId replId, KeeperConfig keeperConfig, File storeDir) {
		
		return createReplicationStoreManager(replId, randomKeeperRunid(), keeperConfig, storeDir);
	}

	protected ReplicationStoreManager createReplicationStoreManager(ReplId replId, String keeperRunid, KeeperConfig keeperConfig, File storeDir) {
		
		DefaultReplicationStoreManager replicationStoreManager = new DefaultReplicationStoreManager(keeperConfig, replId, keeperRunid, storeDir, createkeeperMonitor(), Mockito.mock(SyncRateManager.class), createRedisOpParser(), null, asyncFileSystem());

		replicationStoreManager.addObserver(new Observer() {
			
			@Override
			public void update(Object args, Observable observable) {
				
				if(args instanceof NodeAdded){
					@SuppressWarnings("unchecked")
					ReplicationStore replicationStore = ((NodeAdded<ReplicationStore>) args).getNode();
					try {
						replicationStore.getMetaStore().becomeActive();
					} catch (IOException e) {
						logger.error("[update]" + replicationStore, e);
					}
				}				
			}
		});
		return replicationStoreManager;
	}

	protected RedisOpParser createRedisOpParser() {
		RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
		RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
		return new GeneralRedisOpParser(redisOpParserManager);
	}

	protected KeepersMonitorManager createkeepersMonitorManager(){
		return new NoneKeepersMonitorManager();
	}

	protected KeeperMonitor createkeeperMonitor(){
		return new NoneKeeperMonitor(scheduled);
	}

	protected String randomKeeperRunid(){

		return RunidGenerator.DEFAULT.generateRunid();
	}
	

	protected File getReplicationStoreManagerBaseDir(KeeperMeta keeper) {

		String tmpDir = getTestFileDir();
		return new File(String.format("%s/%s", tmpDir, keeper.getPort()));
	}

	
	protected byte[] readRdbFileTilEnd(ReplicationStore replicationStore) throws IOException, InterruptedException {

		RdbStore rdbStore = ((DefaultReplicationStore)replicationStore).getRdbStore();
		
		return readRdbFileTilEnd(rdbStore);
	}

	protected byte[] readRdbFileTilEnd(RdbStore rdbStore) throws IOException, InterruptedException {

		final ByteArrayWritableByteChannel bachannel = new ByteArrayWritableByteChannel();
		final CountDownLatch latch = new CountDownLatch(1);
		

		rdbStore.readRdbFile(new RdbFileListener() {

			@Override
			public void setRdbFileInfo(EofType eofType, ReplicationProgress<?> rdbProgress) {

			}

			@Override
			public boolean supportProgress(Class<? extends ReplicationProgress<?>> clazz) {
				return true;
			}

			@Override
			public void onFileData(ReferenceFileRegion referenceFileRegion) throws IOException {
				if (referenceFileRegion == null) {
					latch.countDown();
					return;
				}
				referenceFileRegion.transferTo(bachannel, 0L);
			}

			@Override
			public boolean isOpen() {
				return true;
			}

			@Override
			public void exception(Exception e) {
				latch.countDown();
			}

			@Override
			public void beforeFileData() {
			}
		});
		latch.await(5, TimeUnit.SECONDS);
		return bachannel.getResult();
	}

	public String readCommandFileTilEnd(final ReplicationStore replicationStore, int expectedLen) throws IOException {
		
		return readCommandFileTilEnd(0, replicationStore, expectedLen);
	}

	
	public String readCommandFileTilEnd(final long beginOffset, final ReplicationStore replicationStore, int expectedLen) throws IOException {

		final ByteArrayOutputStream baous = new ByteArrayOutputStream();
		new Thread() {
			
			public void run() {
				try {
					doRun();
				} catch (Exception e) {
					logger.error("[run]", e);
				}
			}
			
			private void doRun() throws IOException{
				replicationStore.addCommandsListener(new BacklogOffsetReplicationProgress(replicationStore.backlogBeginOffset() + beginOffset), new CommandsListener() {
					
					@Override
					public boolean isOpen() {
						return true;
					}

					@Override
					public void beforeCommand() {
					}

					@Override
					public Long processedBacklogOffset() {
						return null;
					}

					@Override
					public ChannelFuture onCommand(Object cmd) {
						
						try {
							byte [] message = readFileChannelInfoMessageAsBytes((ReferenceFileRegion) cmd);
							baous.write(message);
						} catch (IOException e) {
							logger.error("[onCommand]" + cmd, e);
						}
						return null;
					}

					@Override
					public void onCommandEnd() {

					}
				});
			}
		}.start();

		int lastSize = baous.size();
		long equalCount = 0;
		long deadline = System.currentTimeMillis() + 10000;
		while (System.currentTimeMillis() < deadline) {
			int currentSize = baous.size();
			if (expectedLen >= 0 && currentSize >= expectedLen) {
				break;
			}
			if (currentSize != lastSize) {
				lastSize = currentSize;
				equalCount = 0;
			} else {
				equalCount++;
			}
			// Stall detection only applies when expected length is unknown.
			if (expectedLen < 0 && equalCount > 100) {
				break;
			}
			sleep(10);
		}
		return new String(baous.toByteArray());
	}

	protected byte[] readFileChannelInfoMessageAsBytes(ReferenceFileRegion referenceFileRegion) {

		try {
			ByteArrayWritableByteChannel bach = new ByteArrayWritableByteChannel(); 
			referenceFileRegion.transferTo(bach, 0L);
			return bach.getResult();
		} catch (IOException e) {
			throw new IllegalStateException(String.format("[read]%s", referenceFileRegion), e);
		}
	}

	protected String readFileChannelInfoMessageAsString(ReferenceFileRegion referenceFileRegion) {

		return new String(readFileChannelInfoMessageAsBytes(referenceFileRegion), Codec.defaultCharset);
	}

	@Override
	protected String getXpipeMetaConfigFile() {
		return null;
	}
}

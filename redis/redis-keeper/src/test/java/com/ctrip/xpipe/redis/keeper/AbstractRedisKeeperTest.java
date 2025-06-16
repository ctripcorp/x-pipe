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
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.monitor.KeepersMonitorManager;
import com.ctrip.xpipe.redis.keeper.monitor.impl.NoneKeepersMonitorManager;
import com.ctrip.xpipe.redis.keeper.monitor.impl.NoneKeepersMonitorManager.NoneKeeperMonitor;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
import io.netty.channel.ChannelFuture;
import org.junit.BeforeClass;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

	protected ClusterId getClusterId() {
		return new ClusterId(currentTestName()  + "-", 0L);
	}

	protected ClusterId getClusterId(Long id) {
		return new ClusterId(currentTestName()  + "-", id);
	}

	protected ReplId getReplId() {
		return new ReplId(currentTestName() + "-", 0L);
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
		
		DefaultReplicationStoreManager replicationStoreManager = new DefaultReplicationStoreManager(keeperConfig, replId, keeperRunid, storeDir, createkeeperMonitor(), Mockito.mock(SyncRateManager.class));
		
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
				replicationStore.addCommandsListener(new OffsetReplicationProgress(replicationStore.beginOffsetWhenCreated() + beginOffset), new CommandsListener() {
					
					@Override
					public boolean isOpen() {
						return true;
					}

					@Override
					public void beforeCommand() {
					}

					@Override
					public Long processedOffset() {
						return null;
					}

					@Override
					public ChannelFuture onCommand(CommandFile currentFile, long filePosition, Object cmd) {
						
						try {
							byte [] message = readFileChannelInfoMessageAsBytes((ReferenceFileRegion) cmd);
							baous.write(message);
						} catch (IOException e) {
							logger.error("[onCommand]" + cmd, e);
						}
						return null;
					}

				});
			}
		}.start();

		int lastSize = baous.size();
		long equalCount = 0;
		while (true) {
			int currentSize = baous.size();
			if(expectedLen >= 0 && currentSize >= expectedLen){
				break;
			}
			if (currentSize != lastSize) {
				lastSize = currentSize;
				equalCount = 0;
			} else {
				equalCount++;
			}
			if (equalCount > 100) {
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

package com.ctrip.xpipe.redis.keeper;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.payload.ByteArrayWritableByteChannel;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.redis.RunidGenerator;
import com.ctrip.xpipe.redis.core.store.CommandsListener;
import com.ctrip.xpipe.redis.core.store.RdbFileListener;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;

import io.netty.channel.ChannelFuture;

/**
 * @author wenchao.meng
 *
 *         Jun 12, 2016
 */
public class AbstractRedisKeeperTest extends AbstractRedisTest {

	protected String getClusterId() {
		return currentTestName() + "-clusterId";
	}

	protected String getShardId() {

		return currentTestName() + "-shardId";
	}

	protected ReplicationStoreManager createReplicationStoreManager(KeeperConfig keeperConfig) {
		
		String tmpDir = getTestFileDir();

		return createReplicationStoreManager(getClusterId(), getShardId(), keeperConfig, new File(tmpDir));
	}

	
	protected ReplicationStoreManager createReplicationStoreManager() {

		String tmpDir = getTestFileDir();

		return createReplicationStoreManager(getClusterId(), getShardId(), new File(tmpDir));
	}

	protected ReplicationStoreManager createReplicationStoreManager(String clusterId, String shardId, File storeDir) {

		return createReplicationStoreManager(clusterId, shardId, getKeeperConfig(), storeDir);
	}
	
	protected KeeperConfig getKeeperConfig() {
		return new TestKeeperConfig();
	}

	protected ReplicationStoreManager createReplicationStoreManager(String clusterId, String shardId, KeeperConfig keeperConfig, File storeDir) {
		
		return new DefaultReplicationStoreManager(keeperConfig, clusterId, shardId, randomKeeperRunid(), storeDir);
	}

	protected ReplicationStoreManager createReplicationStoreManager(String clusterId, String shardId, String keeperRunid, KeeperConfig keeperConfig, File storeDir) {
		
		return new DefaultReplicationStoreManager(keeperConfig, clusterId, shardId, keeperRunid, storeDir);
	}
	
	protected String randomKeeperRunid(){

		return RunidGenerator.DEFAULT.generateRunid();
	}
	

	protected File getReplicationStoreManagerBaseDir() {

		String tmpDir = getTestFileDir();
		return new File(tmpDir);
	}

	
	protected String readRdbFileTilEnd(ReplicationStore replicationStore) throws IOException, InterruptedException {

		final ByteArrayWritableByteChannel bachannel = new ByteArrayWritableByteChannel();
		final CountDownLatch latch = new CountDownLatch(1);

		((DefaultReplicationStore)replicationStore).getRdbStore().readRdbFile(new RdbFileListener() {

			@Override
			public void setRdbFileInfo(long rdbFileSize, long rdbFileOffset) {

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

		latch.await();
		return new String(bachannel.getResult());
	}

	public String readCommandFileTilEnd(final ReplicationStore replicationStore) throws IOException {
		
		return readCommandFileTilEnd(0, replicationStore);
	}

	
	public String readCommandFileTilEnd(final long beginOffset, final ReplicationStore replicationStore) throws IOException {

		final ByteArrayOutputStream baous = new ByteArrayOutputStream();
		new Thread() {
			
			public void run() {
				try {
					doRun();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			private void doRun() throws IOException {
				replicationStore.addCommandsListener(replicationStore.getMetaStore().getKeeperBeginOffset() + beginOffset, new CommandsListener() {
					
					@Override
					public boolean isOpen() {
						return true;
					}

					@Override
					public void beforeCommand() {
					}

					@Override
					public ChannelFuture onCommand(ReferenceFileRegion referenceFileRegion) {
						
						try {
							byte [] message = readFileChannelInfoMessageAsBytes(referenceFileRegion);
							baous.write(message);
						} catch (IOException e) {
							logger.error("[onCommand]" + referenceFileRegion, e);
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
			if (currentSize != lastSize) {
				lastSize = currentSize;
				equalCount = 0;
			} else {
				equalCount++;
			}
			if (equalCount > 10) {
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

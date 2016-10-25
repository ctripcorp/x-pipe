package com.ctrip.xpipe.redis.keeper;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;

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

import io.netty.buffer.ByteBuf;

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
			public void onFileData(FileChannel fileChannel, long pos, long len) throws IOException {
				if (len == -1) {
					latch.countDown();
					return;
				}
				fileChannel.transferTo(pos, len, bachannel);
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

		final List<ByteBuf> buffs = new LinkedList<>();
		final AtomicInteger size = new AtomicInteger();

		new Thread() {
			
			public void run() {
				try {
					doRun();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			private void doRun() throws IOException {
				replicationStore.addCommandsListener(replicationStore.getMetaStore().getKeeperBeginOffset(), new CommandsListener() {
					
					@Override
					public void onCommand(ByteBuf byteBuf) {
						
						buffs.add(byteBuf);
						size.addAndGet(byteBuf.readableBytes());
					}
					
					@Override
					public boolean isOpen() {
						return true;
					}

					@Override
					public void beforeCommand() {
					}
				});
			}
		}.start();

		int lastSize = buffs.size();
		long equalCount = 0;
		while (true) {

			int currentSize = buffs.size();
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

		byte[] result = new byte[size.get()];
		int destIndex = 0;
		for (ByteBuf byteBuf : buffs) {
			int readable = byteBuf.readableBytes();
			byteBuf.readBytes(result, destIndex, readable);
			Assert.assertEquals(0, byteBuf.readableBytes());
			destIndex += readable;
		}

		return new String(result);
	}

	@Override
	protected String getXpipeMetaConfigFile() {
		return null;
	}
}

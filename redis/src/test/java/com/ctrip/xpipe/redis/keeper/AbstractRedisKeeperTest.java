package com.ctrip.xpipe.redis.keeper;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Before;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.ctrip.xpipe.payload.ByteArrayWritableByteChannel;
import com.ctrip.xpipe.redis.AbstractRedisTest;
import com.ctrip.xpipe.redis.keeper.entity.KeeperMeta;
import com.ctrip.xpipe.redis.keeper.entity.RedisMeta;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.meta.MetaServiceManager;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
import com.ctrip.xpipe.redis.spring.KeeperContextConfig;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 *         Jun 12, 2016
 */
public class AbstractRedisKeeperTest extends AbstractRedisTest {

	protected MetaServiceManager metaServiceManager;

	protected AnnotationConfigApplicationContext springCtx;

	private int keeperServerPortMin = 7777, keeperServerPortMax = 7877;

	@Before
	public void beforeAbstractRedisKeeperTest() {

		springCtx = new AnnotationConfigApplicationContext(KeeperContextConfig.class);
		metaServiceManager = springCtx.getBean(MetaServiceManager.class);
	}

	protected RedisKeeperServer createRedisKeeperServer() throws Exception {

		return createRedisKeeperServer(createKeeperMeta());
	}

	protected KeeperMeta createKeeperMeta() {

		return createKeeperMeta(randomPort(keeperServerPortMin, keeperServerPortMax));
	}


	protected KeeperMeta createKeeperMeta(int port) {

		KeeperMeta keeperMeta = new KeeperMeta();
		keeperMeta.setIp("127.0.0.1");
		keeperMeta.setPort(port);
		keeperMeta.setActive(true);
		keeperMeta.setId(randomString(40));
		return keeperMeta;
	}

	protected RedisKeeperServer createRedisKeeperServer(KeeperMeta keeperMeta) throws Exception {

		String clusterId = getClusterId();
		String shardId = getShardId();

		ReplicationStoreManager replicationStoreManager = createReplicationStoreManager(clusterId, shardId);
		return createRedisKeeperServer(clusterId, shardId, keeperMeta, replicationStoreManager, metaServiceManager);
	}

	protected RedisKeeperServer createRedisKeeperServer(String clusterId, String shardId, KeeperMeta keeper,
			ReplicationStoreManager replicationStoreManager, MetaServiceManager metaServiceManager) throws Exception {

		RedisKeeperServer redisKeeperServer = new DefaultRedisKeeperServer(clusterId, shardId, keeper,
				replicationStoreManager, metaServiceManager);

		add(redisKeeperServer);
		return redisKeeperServer;
	}

	/**
	 * user default infor
	 * 
	 * @return
	 */
	protected ReplicationStoreManager createReplicationStoreManager() {

		String tmpDir = getTestFileDir();

		return new DefaultReplicationStoreManager(getClusterId(), getShardId(), new File(tmpDir));
	}

	protected ReplicationStoreManager createReplicationStoreManager(String clusterId, String shardId, File storeDir) {
		return new DefaultReplicationStoreManager(clusterId, shardId, storeDir);
	}

	protected ReplicationStoreManager createReplicationStoreManager(String clusterId, String shardId) {

		String tmpDir = getTestFileDir();

		return new DefaultReplicationStoreManager(clusterId, shardId, new File(tmpDir));
	}
	
	protected RedisMeta createRedisMeta() {
		
		return createRedisMeta("localhost", randomInt());
	}

	protected RedisMeta createRedisMeta(String host, int port) {
		
		RedisMeta redisMeta = new RedisMeta();
		redisMeta.setIp(host);
		redisMeta.setPort(port);
		return redisMeta;
	}
	

	protected String getClusterId() {
		return currentTestName() + "-clusterId";
	}

	protected String getShardId() {

		return currentTestName() + "-shardId";
	}

	protected String readRdbFileTilEnd(ReplicationStore replicationStore) throws IOException, InterruptedException {

		final ByteArrayWritableByteChannel bachannel = new ByteArrayWritableByteChannel();
		final CountDownLatch latch = new CountDownLatch(1);

		replicationStore.readRdbFile(new RdbFileListener() {

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
			public boolean isStop() {
				return false;
			}

			@Override
			public void exception(Exception e) {
				latch.countDown();
			}
		});

		latch.await();
		return new String(bachannel.getResult());
	}

	public String readCommandFileTilEnd(ReplicationStore replicationStore) throws IOException {

		final List<ByteBuf> buffs = new LinkedList<>();
		final AtomicInteger size = new AtomicInteger();

		replicationStore.addCommandsListener(0, new CommandsListener() {

			@Override
			public void onCommand(ByteBuf byteBuf) {

				buffs.add(byteBuf);
				size.addAndGet(byteBuf.readableBytes());
			}
		});

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

}

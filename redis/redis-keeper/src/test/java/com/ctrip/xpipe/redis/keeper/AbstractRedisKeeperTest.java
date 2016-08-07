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
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.xml.sax.SAXException;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.payload.ByteArrayWritableByteChannel;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.store.CommandsListener;
import com.ctrip.xpipe.redis.core.store.RdbFileListener;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.meta.MetaService;
import com.ctrip.xpipe.redis.keeper.spring.KeeperContextConfig;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 *         Jun 12, 2016
 */
public class AbstractRedisKeeperTest extends AbstractRedisTest {

	protected MetaService  metaService;
	
	private String keeperConfigFile = "keeper6666.xml";

	private int keeperServerPortMin = 7777, keeperServerPortMax = 7877;

	@Before
	public void beforeAbstractRedisKeeperTest() throws Exception {
		
		doIdcInit();
		
		metaService = getRegistry().getComponent(MetaService.class);
		
	}
	
	@Override
	protected ApplicationContext createSpringContext() {
		
		return new AnnotationConfigApplicationContext(KeeperContextConfig.class);
	}
	
	

	protected void doIdcInit() {
	}

	protected RedisKeeperServer createRedisKeeperServer() throws Exception {

		return createRedisKeeperServer(createKeeperMeta());
	}

	protected KeeperMeta createKeeperMeta() {

		return createKeeperMeta(randomPort(keeperServerPortMin, keeperServerPortMax));
	}


	protected KeeperMeta createKeeperMeta(int port) {

		try {
			
			XpipeMeta xpipe = DefaultSaxParser.parse(getClass().getClassLoader().getResourceAsStream(keeperConfigFile));
			for(DcMeta dcMeta : xpipe.getDcs().values()){
				for(ClusterMeta clusterMeta : dcMeta.getClusters().values()){
					for(ShardMeta shardMeta : clusterMeta.getShards().values()){
						for(KeeperMeta keeperMeta : shardMeta.getKeepers()){
							keeperMeta.setPort(port);
							keeperMeta.setActive(true);
							keeperMeta.setId(randomString(40));
							return keeperMeta;
						}
					}
				}
			}
		} catch (SAXException | IOException e) {
			throw new IllegalStateException(e);
		}
		
		return null;
	}

	protected RedisKeeperServer createRedisKeeperServer(KeeperMeta keeperMeta) throws Exception {

		String clusterId = getClusterId();
		String shardId = getShardId();

		ReplicationStoreManager replicationStoreManager = createReplicationStoreManager(clusterId, shardId);
		return createRedisKeeperServer(keeperMeta, replicationStoreManager, metaService);
	}

	protected RedisKeeperServer createRedisKeeperServer(KeeperMeta keeper,
			ReplicationStoreManager replicationStoreManager, MetaService metaService) throws Exception {

		RedisKeeperServer redisKeeperServer = new DefaultRedisKeeperServer(keeper,
				replicationStoreManager, metaService, getRegistry().getComponent(LeaderElectorManager.class));

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
		
		return createRedisMeta("localhost", randomPort());
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

	protected String readRdbFileTilEnd(DefaultReplicationStore replicationStore) throws IOException, InterruptedException {

		final ByteArrayWritableByteChannel bachannel = new ByteArrayWritableByteChannel();
		final CountDownLatch latch = new CountDownLatch(1);

		replicationStore.getRdbStore().readRdbFile(new RdbFileListener() {

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
				replicationStore.getCommandStore().addCommandsListener(0, new CommandsListener() {
					
					@Override
					public void onCommand(ByteBuf byteBuf) {
						
						buffs.add(byteBuf);
						size.addAndGet(byteBuf.readableBytes());
					}
					
					@Override
					public boolean isOpen() {
						return true;
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

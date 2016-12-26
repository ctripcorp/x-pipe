package com.ctrip.xpipe.redis.keeper.impl.fakeredis;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.impl.AbstractRedisMasterReplication;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;

import io.netty.buffer.Unpooled;

/**
 * @author wenchao.meng
 *
 *         2016年4月21日 下午5:42:29
 */
public class FakeRedisExceptionTest extends AbstractFakeRedisTest {
	
	private AtomicInteger errorCount = new AtomicInteger();
	private int totalErrorCount = 2;
	private CountDownLatch latch = new CountDownLatch(totalErrorCount + 1);
	
	@Before
	public void beforeExceptionTest(){
		System.setProperty(AbstractRedisMasterReplication.KEY_MASTER_CONNECT_RETRY_DELAY_SECONDS, "1");
		fakeRedisServer.setEof(false);
	}
	
	
	@Test
	public void testRdbFileError() throws Exception{
		
		startRedisKeeperServerAndConnectToFakeRedis();
		
		Assert.assertTrue(latch.await((totalErrorCount + 1) * 2, TimeUnit.SECONDS));;
	}
	
	
	protected RedisKeeperServer createRedisKeeperServer(KeeperMeta keeper, KeeperConfig keeperConfig,
			MetaServerKeeperService metaService, File baseDir, LeaderElectorManager leaderElectorManager) {
		
		return new DefaultRedisKeeperServer(keeper, keeperConfig, baseDir, metaService, leaderElectorManager, createkeeperMonitorManager()){
		
			@Override
			public void beginWriteRdb(EofType eofType, long offset) {
				
				super.beginWriteRdb(eofType, offset);
				latch.countDown();
				try {
					writeToRdb(getCurrentReplicationStore());
				} catch (IOException e) {
					logger.error("[beginWriteRdb][test]", e);
				}
			}
			
		};
	}

	private void writeToRdb(ReplicationStore currentReplicationStore) throws IOException {

		int current = errorCount.incrementAndGet();
		
		if(current > totalErrorCount){
			logger.info("[writeToRdb][no write]");
			return;
		}
		
		logger.info("[writeToRdb][write error byte]");
		DefaultReplicationStore replicationStore = (DefaultReplicationStore) currentReplicationStore;
		replicationStore.getRdbStore().writeRdb(Unpooled.wrappedBuffer("1".getBytes()));
		
	}

	
}

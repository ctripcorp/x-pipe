package com.ctrip.xpipe.redis.keeper.impl.fakeredis;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.AbstractFakeRedisTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.impl.AbstractRedisMasterReplication;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;

/**
 * @author wenchao.meng
 *
 *         2016年4月21日 下午5:42:29
 */
public class FakeRedisExceptionTest extends AbstractFakeRedisTest {

	private CountDownLatch countDownLatch = new CountDownLatch(1);
	
	@Before
	public void beforeExceptionTest(){
		System.setProperty(AbstractRedisMasterReplication.KEY_MASTER_CONNECT_RETRY_DELAY_SECONDS, "1");
		fakeRedisServer.setEof(false);
	}
	
	
	@Test
	public void testRdbFileError() throws Exception{
		
		RedisKeeperServer redisKeeperServer = startRedisKeeperServerAndConnectToFakeRedis();
		
		countDownLatch.await(10, TimeUnit.SECONDS);
		RdbStore rdbStore = ((DefaultReplicationStore)redisKeeperServer.getReplicationStore()).getRdbStore();
		Assert.assertFalse(rdbStore.checkOk());
	}
	
	
	protected RedisKeeperServer createRedisKeeperServer(Long replId,  KeeperMeta keeper, KeeperConfig keeperConfig,
			File baseDir, LeaderElectorManager leaderElectorManager) {
		
		return new DefaultRedisKeeperServer(replId, keeper, keeperConfig, baseDir, leaderElectorManager,
				createkeepersMonitorManager(), getRegistry().getComponent(KeeperResourceManager.class)){
		
			@Override
			public void beginWriteRdb(EofType eofType, String replId, long offset) {
				
				super.beginWriteRdb(eofType, replId, offset);
				try {
					writeToRdb(getCurrentReplicationStore());
				} catch (IOException e) {
					logger.error("[beginWriteRdb][test]", e);
				}
			}
			
			@Override
			public void endWriteRdb() {
				super.endWriteRdb();
				countDownLatch.countDown();
			}
			
		};
	}

	private void writeToRdb(ReplicationStore currentReplicationStore) throws IOException {
		
		logger.info("[writeToRdb][write error byte]");
		DefaultReplicationStore replicationStore = (DefaultReplicationStore) currentReplicationStore;
		replicationStore.getRdbStore().writeRdb(Unpooled.wrappedBuffer("1".getBytes()));
		
	}

	
}

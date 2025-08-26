package com.ctrip.xpipe.redis.integratedtest.keeper.manul;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.integratedtest.keeper.AbstractKeeperIntegratedSingleDc;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.monitor.KeepersMonitorManager;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * Sep 29, 2016
 */
public class BadKeeper extends AbstractKeeperIntegratedSingleDc{
	
	private volatile boolean done = false; 
	
	@Test
	public void startTest() throws IOException{
		
		try{
			sendMessageToMasterAndTestSlaveRedis(10);
		}catch(Throwable e){
			logger.error("[startTest]", e);
		}
		
		waitForAnyKeyToExit();
	}
	
	
	
	@Override
	protected RedisKeeperServer createRedisKeeperServer(KeeperMeta keeperMeta, File baseDir, KeeperConfig keeperConfig,
														LeaderElectorManager leaderElectorManager, KeepersMonitorManager keeperMonitorManager,
														SyncRateManager syncRateManager) {
		
		return new DefaultRedisKeeperServer(keeperMeta.parent().getDbId(), keeperMeta, keeperConfig, baseDir, leaderElectorManager,
				keeperMonitorManager, resourceManager, syncRateManager, generateRedisOpParser()){
			@Override
			public void endWriteRdb() {
				super.endWriteRdb();
				
				if(!done){
					try {
						getReplicationStore().appendCommands(Unpooled.wrappedBuffer(randomString(100 * 1024).getBytes()));
						getReplicationStore().appendCommands(Unpooled.wrappedBuffer("helloworld".getBytes()));
						getReplicationStore().appendCommands(Unpooled.wrappedBuffer("set a b \r\n".getBytes()));
					} catch (IOException e) {
						logger.error("[endWriteRdb][give wrong commands]", e);
					}
					done = true;
				}
				
			}
		};
	}
	

	@Override
	protected KeeperConfig getKeeperConfig() {
		return new TestKeeperConfig(1 << 30, 5, 1 << 30, 300000);
	}

}

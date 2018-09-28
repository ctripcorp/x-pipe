package com.ctrip.xpipe.redis.keeper.impl.fakeredis;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.SequenceCommandChain;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.FixedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.CAPA;
import com.ctrip.xpipe.redis.core.protocal.PsyncObserver;
import com.ctrip.xpipe.redis.core.protocal.cmd.InMemoryPsync;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf.ReplConfType;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.server.FakeRedisServer;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperContextTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * Oct 18, 2016
 */
public class AbstractFakeRedisTest extends AbstractRedisKeeperContextTest{
	
	protected FakeRedisServer fakeRedisServer;

	protected int commandFileSize;
	protected int allCommandsSize; 

	@Before
	public void beforeAbstractFakeRedisTest() throws Exception{
		fakeRedisServer = startFakeRedisServer();
		
		allCommandsSize = fakeRedisServer.getCommandsLength();
		commandFileSize = fakeRedisServer.getSendBatchSize();
	}
	
	
	
	protected RedisKeeperServer startRedisKeeperServerAndConnectToFakeRedis() throws Exception {
		return startRedisKeeperServerAndConnectToFakeRedis(100, allCommandsSize);
	}


	protected RedisKeeperServer startRedisKeeperServerAndConnectToFakeRedis(int replicationStoreCommandFileNumToKeep, 
			int replicationStoreMaxCommandsToTransferBeforeCreateRdb) throws Exception {
		return startRedisKeeperServerAndConnectToFakeRedis(replicationStoreCommandFileNumToKeep, replicationStoreMaxCommandsToTransferBeforeCreateRdb, 1000);
	}

	protected RedisKeeperServer startRedisKeeperServerAndConnectToFakeRedis(int replicationStoreCommandFileNumToKeep, 
			int replicationStoreMaxCommandsToTransferBeforeCreateRdb, int minTimeMilliToGcAfterCreate) throws Exception {

		RedisKeeperServer redisKeeperServer = startRedisKeeperServer(replicationStoreCommandFileNumToKeep, replicationStoreMaxCommandsToTransferBeforeCreateRdb, 1000);
		connectToFakeRedis(redisKeeperServer);
		return redisKeeperServer;
	}

	

	protected RedisKeeperServer startRedisKeeperServer() throws Exception {
		return startRedisKeeperServer(100, allCommandsSize, 1000);
	}

	protected RedisKeeperServer startRedisKeeperServer(int replicationStoreCommandFileNumToKeep, 
			int replicationStoreMaxCommandsToTransferBeforeCreateRdb, int minTimeMilliToGcAfterCreate) throws Exception {
		
		KeeperConfig keeperConfig = new TestKeeperConfig(
				commandFileSize, 
				replicationStoreCommandFileNumToKeep, 
				replicationStoreMaxCommandsToTransferBeforeCreateRdb, minTimeMilliToGcAfterCreate);
		RedisKeeperServer redisKeeperServer = createRedisKeeperServer(keeperConfig);
		redisKeeperServer.initialize();
		redisKeeperServer.start();
		add(redisKeeperServer);
		
		return redisKeeperServer;
	}

	protected void connectToFakeRedis(RedisKeeperServer redisKeeperServer) {
		redisKeeperServer.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("localhost", fakeRedisServer.getPort()));
		
	}

	protected void assertPsyncResultEquals(InMemoryPsync psync) {

		try{
			Assert.assertEquals(fakeRedisServer.getRdbContent(), new String(psync.getRdb()));
			Assert.assertEquals(fakeRedisServer.currentCommands(), new String(psync.getCommands()));
		}catch(Exception e){
			logger.error("[assertPsyncResultEquals]", e);
		}

		Assert.assertEquals(fakeRedisServer.getRdbContent(), new String(psync.getRdb()));
		Assert.assertEquals(fakeRedisServer.currentCommands(), new String(psync.getCommands()));
	}

	@Override
	protected String getXpipeMetaConfigFile() {
		return "keeper-test.xml";
	}

	protected InMemoryPsync sendInmemoryPsync(String ip, int port) throws Exception {

		return sendInmemoryPsync(ip, port, "?", -1);
	}

	protected InMemoryPsync sendInmemoryPsync(String ip, int port, String runid, long offset) throws Exception {

		SequenceCommandChain chain = new SequenceCommandChain(false);
		
		SimpleObjectPool<NettyClient> pool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(ip, port));
		NettyClient nettyClient = null;
		
		try{
			nettyClient = pool.borrowObject();
			
			SimpleObjectPool<NettyClient> clientPool = new FixedObjectPool<NettyClient>(nettyClient);  
			chain.add(new Replconf(clientPool, 
					ReplConfType.CAPA, scheduled, CAPA.EOF.toString()));
			InMemoryPsync psync = new InMemoryPsync(clientPool, runid, offset, scheduled);
			chain.add(psync);
			
			psync.addPsyncObserver(new PsyncObserver() {
				
				private long masterRdbOffset = 0;
				@Override
				public void reFullSync() {
					
				}
				
				@Override
				public void onFullSync() {
					
				}
				
				@Override
				public void onContinue(String requestReplId, String responseReplId) {
					
				}
				
				@Override
				public void endWriteRdb() {
					new Replconf(clientPool, ReplConfType.ACK, scheduled, String.valueOf(masterRdbOffset)).execute();
				}
				
				@Override
				public void beginWriteRdb(EofType eofType, long masterRdbOffset) throws IOException {
					this.masterRdbOffset = masterRdbOffset;
				}
			});
			
			chain.execute();
			return psync;
		}finally{
			if(nettyClient != null){
				pool.returnObject(nettyClient);
			}
		}
	}
}

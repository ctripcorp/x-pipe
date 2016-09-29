package com.ctrip.xpipe.redis.keeper.impl;

import org.junit.Assert;

import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;

import java.net.InetSocketAddress;

import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.server.FakeRedisServer;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperContextTest;
import com.ctrip.xpipe.redis.keeper.RdbDumper;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;

/**
 * @author wenchao.meng
 *
 *         2016年4月21日 下午5:42:29
 */
public class DefaultRedisKeeperServerTest extends AbstractRedisKeeperContextTest {
	
	@Before
	public void beforeDefaultRedisKeeperServerTest() throws Exception {
		
	}
	
	@Test
	public void testRdbDumperTooQuick() throws Exception{
		
		int rdbDumpMinIntervalMilli = 100;
		TestKeeperConfig keeperConfig = new TestKeeperConfig();
		keeperConfig.setRdbDumpMinIntervalMilli(rdbDumpMinIntervalMilli);
		RedisKeeperServer redisKeeperServer = createRedisKeeperServer(keeperConfig);
		
		RdbDumper dump1 = mock(RdbDumper.class);
		
		redisKeeperServer.setRdbDumper(dump1);

		redisKeeperServer.clearRdbDumper(dump1);
		

		//too quick
		//force can success
		redisKeeperServer.setRdbDumper(dump1, true);
		redisKeeperServer.clearRdbDumper(dump1);
		
		try{
			redisKeeperServer.setRdbDumper(dump1);
			Assert.fail();
		}catch(SetRdbDumperException e){
		}
		
		sleep(rdbDumpMinIntervalMilli * 2);
		redisKeeperServer.setRdbDumper(dump1);
	}
	
	@Test
	public void testKeeperStopNoConnectMaster() throws Exception{
		FakeRedisServer server1 = startFakeRedisServer();
		FakeRedisServer server2 = startFakeRedisServer();
		FakeRedisServer server3 = startFakeRedisServer();
		
		RedisKeeperServer redisKeeperServer = createRedisKeeperServer();
		
		redisKeeperServer.initialize();
		redisKeeperServer.start();
		
		redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateActive(redisKeeperServer, new InetSocketAddress(server1.getPort())));
		redisKeeperServer.reconnectMaster();
		sleep(50);
		Assert.assertEquals(1, server1.getConnected());
		
		redisKeeperServer.stop();
		
		redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateActive(redisKeeperServer, new InetSocketAddress(server2.getPort())));
		redisKeeperServer.reconnectMaster();
		sleep(50);
		Assert.assertEquals(0, server1.getConnected());
		Assert.assertEquals(0, server2.getConnected());
		
		redisKeeperServer.dispose();
		
		redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateActive(redisKeeperServer, new InetSocketAddress(server3.getPort())));
		redisKeeperServer.reconnectMaster();
		sleep(50);
		Assert.assertEquals(0, server1.getConnected());
		Assert.assertEquals(0, server2.getConnected());
		Assert.assertEquals(0, server3.getConnected());
	}
	

	
	@Test
	public void testKeeperServerInitState() throws Exception{
		
		RedisKeeperServer redisKeeperServer = createRedisKeeperServer();
		redisKeeperServer.initialize();
		
		Assert.assertEquals(KeeperState.UNKNOWN, redisKeeperServer.getRedisKeeperServerState().keeperState());
		
		redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateActive(redisKeeperServer));
		redisKeeperServer.dispose();

		redisKeeperServer.onContinue();
		
		redisKeeperServer = createRedisKeeperServer();
		redisKeeperServer.initialize();
		Assert.assertEquals(KeeperState.PRE_ACTIVE, redisKeeperServer.getRedisKeeperServerState().keeperState());
		
		
		redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateBackup(redisKeeperServer));
		redisKeeperServer.dispose();
		
		redisKeeperServer.onContinue();
		
		redisKeeperServer = createRedisKeeperServer();
		redisKeeperServer.initialize();
		Assert.assertEquals(KeeperState.PRE_BACKUP, redisKeeperServer.getRedisKeeperServerState().keeperState());
	}
	
	
	
	@Override
	protected String getXpipeMetaConfigFile() {
		return "keeper-test.xml";
	}
	

}

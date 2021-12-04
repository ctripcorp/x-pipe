package com.ctrip.xpipe.redis.keeper.handler;

import com.ctrip.xpipe.redis.core.redis.RunidGenerator;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.KeeperRepl;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.impl.DefaultKeeperMonitor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

/**
 * @author wenchao.meng
 *
 * Feb 20, 2017
 */
@RunWith(MockitoJUnitRunner.class)
public class PsyncHandlerTest extends AbstractRedisKeeperTest{
	
	private PsyncHandler psyncHandler;
	
	private String function;
	
	public static final String PARTIAL = "doPartialSync";
	public static final String FULL = "doFullSync";
	public static final String WAIT = "waitForoffset";
	public static final String KEEPER_PARTIAL = "doKeeperPartialSync";
	
	
	private String replId = RunidGenerator.DEFAULT.generateRunid();
	private String replId2 = RunidGenerator.DEFAULT.generateRunid();
	private long secondReplOffset = 10000L;
	private long begin = 2L;
	private long end = 1000000L;
	
	private long maxToTransfer = Long.MAX_VALUE; 
	
	@Mock
	private RedisSlave redisSlave;
	@Mock
	private RedisKeeperServer redisKeeperServer;
	@Mock
	private KeeperRepl keeperRepl;
	@Mock
	private KeeperConfig KeeperConfig;
	
	@Before
	public void beforePsyncHandlerTest(){

		when(redisSlave.getRedisKeeperServer()).thenReturn(redisKeeperServer);
		when(redisKeeperServer.getKeeperRepl()).thenReturn(keeperRepl);
		when(redisKeeperServer.getShardId()).thenReturn(getShardId());
		DefaultKeeperMonitor monitor = new DefaultKeeperMonitor(redisKeeperServer, scheduled);
		when(redisKeeperServer.getKeeperMonitor()).thenReturn(monitor);
		when(redisKeeperServer.getKeeperConfig()).thenReturn(KeeperConfig);
		when(keeperRepl.getBeginOffset()).thenReturn(begin);
		when(keeperRepl.getEndOffset()).thenReturn(end);
		when(keeperRepl.replId()).thenReturn(replId);
		when(keeperRepl.replId2()).thenReturn(replId2);
		when(keeperRepl.secondReplIdOffset()).thenReturn(secondReplOffset);
		
		when(KeeperConfig.getReplicationStoreMaxCommandsToTransferBeforeCreateRdb()).thenReturn(maxToTransfer);
		
		psyncHandler = new PsyncHandler(){
			@Override
			protected void doPartialSync(RedisSlave redisSlave, String replId, Long offset) {
				function = PARTIAL;
			}
			@Override
			protected void doFullSync(RedisSlave redisSlave) {
				function = FULL;
			}
			@Override
			protected void waitForoffset(String[] args, RedisSlave redisSlave, String replId, Long offsetRequest) {
				function = WAIT;
			}

			@Override
			protected void doKeeperPartialSync(RedisSlave redisSlave, String replId, long continueOffset) {
				function = KEEPER_PARTIAL;
			}
		};
	}
	
	
	@Test
	public void testFull(){

		String args[] = new String[]{
				"?",
				"-1"
		};
		
		psyncHandler.innerDoHandle(args, redisSlave, redisKeeperServer);
		Assert.assertEquals(FULL, function);

		
		args = new String[]{
				replId,
				String.valueOf((begin - 1))
		};
		psyncHandler.innerDoHandle(args, redisSlave, redisKeeperServer);
		Assert.assertEquals(FULL, function);

		args = new String[]{
				randomString(40),
				String.valueOf((begin - 1))
		};
		psyncHandler.innerDoHandle(args, redisSlave, redisKeeperServer);
		Assert.assertEquals(FULL, function);

		args = new String[]{
				replId2,
				String.valueOf(secondReplOffset + 1)
		};
		psyncHandler.innerDoHandle(args, redisSlave, redisKeeperServer);
		Assert.assertEquals(FULL, function);
	}
	
	@Test
	public void testPartial(){
		
		String args[] = new String[]{
				replId,
				String.valueOf(begin)
		};
		
		psyncHandler.innerDoHandle(args, redisSlave, redisKeeperServer);
		Assert.assertEquals(PARTIAL, function);

		args = new String[]{
				replId,
				String.valueOf(begin + 1)
		};
		psyncHandler.innerDoHandle(args, redisSlave, redisKeeperServer);
		Assert.assertEquals(PARTIAL, function);

		args = new String[]{
				replId,
				String.valueOf(end + 1)
		};
		psyncHandler.innerDoHandle(args, redisSlave, redisKeeperServer);
		Assert.assertEquals(PARTIAL, function);
		

		args = new String[]{
				replId2,
				String.valueOf(secondReplOffset)
		};
		psyncHandler.innerDoHandle(args, redisSlave, redisKeeperServer);
		Assert.assertEquals(PARTIAL, function);
	}
	
	@Test
	public void testWait(){

		String args[] = new String[]{
				replId,
				String.valueOf(end + 2)
		};
		psyncHandler.innerDoHandle(args, redisSlave, redisKeeperServer);
		Assert.assertEquals(WAIT, function);
	}
	
	@Test
	public void testTooMuchCommands(){
		
		String args[] = new String[]{
				replId,
				String.valueOf(begin)
		};
		
		
		long transfter = end - begin;
		when(KeeperConfig.getReplicationStoreMaxCommandsToTransferBeforeCreateRdb()).thenReturn(transfter);
		psyncHandler.innerDoHandle(args, redisSlave, redisKeeperServer);
		Assert.assertEquals(FULL, function);

		transfter = end - begin + 1;
		when(KeeperConfig.getReplicationStoreMaxCommandsToTransferBeforeCreateRdb()).thenReturn(transfter);
		psyncHandler.innerDoHandle(args, redisSlave, redisKeeperServer);
		Assert.assertEquals(PARTIAL, function);

	}

	@Test
	public void testKeeperPsync() {
		String args[] = new String[]{
				"?",
				"-2"
		};

		when(redisSlave.isKeeper()).thenReturn(true);
		psyncHandler.innerDoHandle(args, redisSlave, redisKeeperServer);
		Assert.assertEquals(KEEPER_PARTIAL, function);

		when(keeperRepl.replId()).thenReturn(null);
		psyncHandler.innerDoHandle(args, redisSlave, redisKeeperServer);
		Assert.assertEquals(FULL, function);
	}

}

package com.ctrip.xpipe.redis.core.protocal.cmd.manual;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractSentinelCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractSentinelCommand.SentinelAdd;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractSentinelCommand.SentinelRemove;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractSentinelCommand.Sentinels;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
public class SentinelCommandTest extends AbstractCommandTest{
	
	private String host = "10.3.2.220";
	
	private int port = 5002;
	
	private String masterName = "sitemon-xpipegroup0";
	
	private SimpleObjectPool<NettyClient> clientPool;
	
	@Before
	public void beforeSentinelCommandTest() throws Exception{
		clientPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(host, port));
	}

	@Test
	public void testSentinels() throws InterruptedException, ExecutionException, Exception{
		
		List<Sentinel> sentinels = getSentinels();
		logger.info("{}", sentinels);
	}
	
	private List<Sentinel> getSentinels() throws InterruptedException, ExecutionException {
		
		return new Sentinels(clientPool, masterName, scheduled).execute().get();
	}

	@Test
	public void testRemove() throws InterruptedException, ExecutionException{
		
		try{
			new SentinelRemove(clientPool, masterName, scheduled).execute().get();
		}catch(Exception e){
			logger.error("[testRemove]", e);
		}
		
		String addResult = new SentinelAdd(clientPool, masterName, "127.0.0.1", 6379, 3, scheduled).execute().get();
		logger.info("{}", addResult);
		
		String removeResult = new SentinelRemove(clientPool, masterName, scheduled).execute().get();
		logger.info("{}", removeResult);

		new SentinelAdd(clientPool, masterName, "127.0.0.1", 6379, 3, scheduled).execute().get();
		logger.info("{}", addResult);

		logger.info("{}", getSentinels());
	}

	@Test
	public void testSentinelMaster() throws InterruptedException, ExecutionException {
		try {
			String addResult = new SentinelAdd(clientPool, masterName, "127.0.0.1", 6379, 3, scheduled).execute().get();
			logger.info("{}", addResult);
		}catch (Exception ignore) {

		}

		try {
			HostPort master = new AbstractSentinelCommand.SentinelMaster(clientPool, scheduled, masterName)
					.execute()
					.get().getHostPort();
			logger.info("[master]{}", master);
			Assert.assertEquals("127.0.0.1", master.getHost());
			Assert.assertEquals(6379, master.getPort());
		} catch (Exception e) {
			logger.error("[testSentinelMaster]", e);
		}

		try{
			new SentinelRemove(clientPool, masterName, scheduled).execute().get();
		}catch(Exception e){
			logger.error("[testRemove]", e);
		}
	}

	@Test
	public void testSentinelMasterTime() throws InterruptedException, ExecutionException {
		long begin = System.currentTimeMillis();
		for(int i = 0; i < 100; i++) {
			try {
				new AbstractSentinelCommand.Sentinels(clientPool, masterName, scheduled).execute().get();
//				logger.info("[master]{}", master);
			} catch (Exception e) {
				logger.error("[testSentinelMaster]", e);
			}
		}
		long end = System.currentTimeMillis();
		logger.info("[duration] {}", end - begin);
	}

	@Test
	public void testSentinelSlaves() throws ExecutionException, InterruptedException {
		List<HostPort> slaves = new AbstractSentinelCommand.SentinelSlaves(clientPool, scheduled, masterName).execute().get();
		logger.info("[testSentinelSlaves] slaves: {}", slaves);
	}
	
	@Test
	public void test(){
		
		String result = String.format("%s %s", 1);
		logger.info("{}", result);
	}
}

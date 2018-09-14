package com.ctrip.xpipe.redis.core.protocal.cmd.transaction;


import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * manual test for local redis
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
public class TransactionalSlaveOfCommandTest extends AbstractRedisTest{
	
	private String ip = "localhost";
	
	private int port = 6379;
	
	private int testCount = 10;
	
	@SuppressWarnings("deprecation")
	@Test
	public void testXslaveof() throws Exception{
		
		XpipeNettyClientKeyedObjectPool pool = getXpipeNettyClientKeyedObjectPool();
		
		for(int i=0; i < testCount; i++){
			
			logger.info(remarkableMessage("{}"), i);
			TransactionalSlaveOfCommand command = new TransactionalSlaveOfCommand(getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(ip, port)), ip, port, scheduled);
			
			Object []result = command.execute().get();
			logger.info("{}", (Object)result);
			
			Assert.assertEquals(0, pool.getObjectPool(new DefaultEndPoint(ip, port)).getNumActive());
			Assert.assertEquals(1, pool.getObjectPool(new DefaultEndPoint(ip, port)).getNumIdle());
		}
		
	}

	@Test
	public void testSaveof(){
		
	}

}

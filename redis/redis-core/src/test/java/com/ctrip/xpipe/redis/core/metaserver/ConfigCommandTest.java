package com.ctrip.xpipe.redis.core.metaserver;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.protocal.cmd.ConfigGetCommand.ConfigGetMinSlavesToWrite;
import com.ctrip.xpipe.redis.core.protocal.cmd.ConfigSetCommand.ConfigSetMinSlavesToWrite;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

/**
 * @author wenchao.meng
 *
 * Dec 2, 2016
 */
public class ConfigCommandTest extends AbstractRedisTest{
	
	private String ip = "localhost";
	
	private int port = 6379;
	
	@Test
	public void testConfigMinSlaves() throws InterruptedException, ExecutionException, Exception{

		DefaultEndPoint address = new DefaultEndPoint(ip, port);
		
		Integer min = new ConfigGetMinSlavesToWrite(getXpipeNettyClientKeyedObjectPool().getKeyPool(address), scheduled).execute().get();
		logger.info("{}", min);
		Boolean result = new ConfigSetMinSlavesToWrite(getXpipeNettyClientKeyedObjectPool().getKeyPool(address), 100, scheduled).execute().get();
		logger.info("{}", result);
		
		min = new ConfigGetMinSlavesToWrite(getXpipeNettyClientKeyedObjectPool().getKeyPool(address), scheduled).execute().get();
		logger.info("{}", min);
		
	}


}

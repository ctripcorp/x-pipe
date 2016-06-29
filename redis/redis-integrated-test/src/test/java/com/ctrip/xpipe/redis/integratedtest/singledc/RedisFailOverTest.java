package com.ctrip.xpipe.redis.integratedtest.singledc;


import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.commons.exec.ExecuteException;
import org.junit.After;
import org.junit.Test;

import com.ctrip.xpipe.redis.keeper.exception.RedisSlavePromotionException;
import com.ctrip.xpipe.redis.meta.server.exception.RedisMetaServerException;

/**
 * @author wenchao.meng
 *
 * Jun 13, 2016
 */
public class RedisFailOverTest extends AbstractSingleDcTest{
	
	
	@Test
	public void testRedisFailover() throws ExecuteException, IOException, RedisSlavePromotionException, RedisMetaServerException, InterruptedException, ExecutionException{
		
		failOverTestTemplate();
	}
	
	@After
	public void afterRedisFailOverTest() throws IOException{
	}
}

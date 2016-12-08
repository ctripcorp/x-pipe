package com.ctrip.xpipe.redis.integratedtest.full.singledc;


import java.io.IOException;
import org.junit.After;
import org.junit.Test;


/**
 * @author wenchao.meng
 *
 * Jun 13, 2016
 */
public class RedisFailOverTest extends AbstractSingleDcTest{
	
	
	@Test
	public void testRedisFailover() throws Exception{
		
		failOverTestTemplate();
	}
	
	@After
	public void afterRedisFailOverTest() throws IOException{
	}
}

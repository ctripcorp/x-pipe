package com.ctrip.xpipe.redis.integratedtest.full.multidc;


import org.junit.Test;


/**
 * @author wenchao.meng
 *
 * Jun 21, 2016
 */
public class RedisFailOverTest extends AbstractMultiDcTest{
	
	@Test
	public void testFailOver() throws Exception{
		
		failOverTestTemplate();

	}
}

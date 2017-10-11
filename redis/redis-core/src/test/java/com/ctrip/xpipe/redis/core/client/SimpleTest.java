package com.ctrip.xpipe.redis.core.client;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import org.junit.Test;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public class SimpleTest extends AbstractRedisTest{
	
	
	@Test
	public void test() throws Exception{
		
		RedisMeta slave = new RedisMeta();
		slave.setIp("localhost");
		slave.setPort(6379);
		System.out.println(getRedisServerRole(slave));
		
	}

}

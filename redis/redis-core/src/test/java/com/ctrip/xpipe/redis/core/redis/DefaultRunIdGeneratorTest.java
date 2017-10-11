package com.ctrip.xpipe.redis.core.redis;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * @author wenchao.meng
 *
 * Aug 19, 2016
 */
public class DefaultRunIdGeneratorTest extends AbstractRedisTest{
	
	private int count = 1 << 10;
	
	@Test
	public void testGenerate(){
		
		Set<String> ids = new HashSet<>();
		
		DefaultRunIdGenerator defaultRunIdGenerator = new DefaultRunIdGenerator();
		for(int i=0;i<count;i++){
			ids.add(defaultRunIdGenerator.generateRunid());
		}
		Assert.assertEquals(count, ids.size());
	}

}

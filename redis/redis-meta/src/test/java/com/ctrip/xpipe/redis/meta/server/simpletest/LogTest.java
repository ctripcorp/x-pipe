package com.ctrip.xpipe.redis.meta.server.simpletest;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;


/**
 * @author wenchao.meng
 *
 * Aug 10, 2016
 */
public class LogTest extends AbstractMetaServerTest{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	@Test
	public void testHash(){
		
		System.out.println("cluster_ly".hashCode()%256);
		
	}
		
	@Test
	public void test() throws IOException{
		
		logger.info("[info]");
		
		logger.error("[error]", new Exception());
		
		waitForAnyKeyToExit();
	}
	
	@Test
	public void testSetSub(){
		
		Set<Integer> set1 = new HashSet<>();
		set1.add(1);set1.add(2);set1.add(3);set1.add(4);

		Set<Integer> set2 = new HashSet<>();
		set2.add(1);set2.add(2);set2.add(3);set2.add(5);
		
		set1.removeAll(set2);
		
		System.out.println(set1);
		
		

		
	}

}

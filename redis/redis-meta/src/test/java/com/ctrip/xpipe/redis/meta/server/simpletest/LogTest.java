package com.ctrip.xpipe.redis.meta.server.simpletest;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.core.config.plugins.util.PluginManager;
import org.apache.logging.log4j.core.config.plugins.util.PluginType;
import org.apache.logging.log4j.core.pattern.PatternConverter;
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

		Class<?> clazz = getClass();

		System.out.println(clazz);

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
	
	@Test
	public void testPlugin(){
		
		PluginManager pm = new PluginManager(PatternConverter.CATEGORY);
		pm.collectPlugins();
		for(Entry<String, PluginType<?>> entry : pm.getPlugins().entrySet()){
			
			logger.info("{} : {}", entry.getKey(), entry.getValue());
			
		}
		
		logger.error("[testPlugin]", new IOException("io exception message..."));
	}

}

package com.ctrip.xpipe.redis.keeper.simple;

import java.util.Calendar;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;

/**
 * @author wenchao.meng
 *
 * May 18, 2016 4:44:03 PM
 */
public class SimpleTest extends AbstractTest{
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	@Test
	public void testFormat(){
		
		logger.info("nihaoma");
		
		System.out.println(String.format("%,d", 111111111));
		System.out.println(String.format("%1$s", "a b c"));
		System.out.println(String.format("%s", "a b c"));
//		System.out.println(String.format("%2$tm %2$te,%2$tY", Calendar.getInstance()));
		
		Calendar calendar = Calendar.getInstance();
		System.out.println(calendar);
//		logger.printf(Level.INFO, "%2$tm %2$te,%2$tY", calendar);
		
	}
	
	@Test
	public void testJson(){
		
		ReplicationStoreMeta meta = new ReplicationStoreMeta();
		meta.setActive(true);
		meta.setBeginOffset(100);
		meta.setMasterRunid("abdc");
		
		String json = JSON.toJSONString(meta);
		
		System.out.println(json);
		
		meta = JSON.parseObject(null, ReplicationStoreMeta.class);
		System.out.println(meta);
		
	}

}

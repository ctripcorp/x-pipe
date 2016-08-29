package com.ctrip.xpipe.redis.keeper.simple;

import java.io.IOException;
import java.util.Calendar;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;

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
		meta.setBeginOffset(100L);
		meta.setMasterRunid("abdc");
		
		String json = JSON.toJSONString(meta);
		
		System.out.println(json);
		
		meta = JSON.parseObject(null, ReplicationStoreMeta.class);
		System.out.println(meta);
		
	}
	
	
	@Test
	public void testCat() throws IOException{
		
		Transaction t1 = Cat.newTransaction("type1", "name1");
			Transaction t21 = Cat.newTransaction("type21", "name2");
				Transaction t31 = Cat.newTransaction("type31", "name3");
				t31.setStatus(Transaction.SUCCESS);
				t31.complete();
			t21.setStatus(Transaction.SUCCESS);
			t21.complete();

			Transaction t22 = Cat.newTransaction("type22", "name2");
			t22.setStatus(Transaction.SUCCESS);
			t22.complete();
		t1.setStatus(Transaction.SUCCESS);	
		t1.complete();

		
		waitForAnyKeyToExit();
	}



}

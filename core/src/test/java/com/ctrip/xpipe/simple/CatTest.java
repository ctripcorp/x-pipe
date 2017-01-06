package com.ctrip.xpipe.simple;

import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.dianping.cat.Cat;

/**
 * @author wenchao.meng
 *
 * Dec 6, 2016
 */
public class CatTest extends AbstractTest{
	
	@Test
	public void testLog(){
		
		Cat.getProducer().logError(new Exception());
	}
	
	@Test
	public void testEnable() {
		Cat.newTransaction("test", "test");
	}
	
	@Test
	public void testDisable() {
		System.setProperty("cat.client.enabled", "false");
		Cat.newTransaction("test", "test");
	}

}

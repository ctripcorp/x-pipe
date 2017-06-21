package com.ctrip.xpipe.simple;

import java.io.IOException;

import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.monitor.CatConfig;
import com.dianping.cat.Cat;

/**
 * @author wenchao.meng
 *
 * Dec 6, 2016
 */
public class CatTest extends AbstractTest{
	
	@Test
	public void testLog() throws IOException{
		
		System.setProperty(CatConfig.CAT_ENABLED_KEY, "true");
		
		Cat.getProducer().logError(new Exception());
		
		waitForAnyKeyToExit();
	}
	
	@Test
	public void testEnable() {
		Cat.newTransaction("test", "test");
	}
	
	@Test
	public void testDisable() {
		
		System.setProperty(CatConfig.CAT_ENABLED_KEY, "false");
		Cat.newTransaction("test", "test");
	}

}

package com.ctrip.xpipe.monitor;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.monitor.Task;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * Jan 3, 2017
 */
public class CatTransactionMonitorTest extends AbstractTest{
	
	@Before
	public void before(){
		System.setProperty(CatConfig.CAT_ENABLED_KEY, "true");

	}
	
	
	@Test
	public void testSuccess() throws Throwable{
		
		CatTransactionMonitor monitor = new CatTransactionMonitor();
		
		monitor.logTransaction("test", getTestName(), new Task() {
			
			@Override
			public void go() throws Exception {
				
			}
		});
		
	}

	
	@Test
	public void testFailure() throws Throwable{
		
		CatTransactionMonitor monitor = new CatTransactionMonitor();
		
		monitor.logTransaction("test", getTestName(), new Task() {
			
			@Override
			public void go() throws Exception {
				throw new Exception("just fail");
			}
		});
	}
	
	@After
	public void afterCatTransactionMonitorTest() throws IOException{
		waitForAnyKeyToExit();
	}

}

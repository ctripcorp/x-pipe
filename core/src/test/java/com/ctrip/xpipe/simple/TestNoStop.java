package com.ctrip.xpipe.simple;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Test;

/**
 * @author wenchao.meng
 *
 * Jan 6, 2017
 */
public class TestNoStop extends AbstractTest{
	
	@Test
	public void test() throws InterruptedException{
		
		Thread thread = 
		new Thread(new Runnable() {
			
			@Override
			public void run() {

				run();
			}
		});
		
		thread.start();
		
		
		thread.join(100);
		
		
		
	}

}

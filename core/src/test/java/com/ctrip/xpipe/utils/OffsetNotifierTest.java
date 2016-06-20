package com.ctrip.xpipe.utils;


import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.utils.OffsetNotifier;

/**
 * @author wenchao.meng
 *
 * May 25, 2016
 */
public class OffsetNotifierTest extends AbstractTest{
	
	private long initPos = 1 << 10 ;
	private int waitMili = 100;
	
	private OffsetNotifier offsetNotifier = new OffsetNotifier(initPos);
	
	@Test
	public void testPass() throws InterruptedException{
		
		for(long i = 0; i <= initPos; i++){
			
			long current = System.currentTimeMillis();
			offsetNotifier.await(i, waitMili);
			long ended = System.currentTimeMillis();
			
			Assert.assertTrue(ended - current <= 10);
		}

		long current = System.currentTimeMillis();
		offsetNotifier.await(initPos + 1, waitMili);
		long ended = System.currentTimeMillis();
		
		Assert.assertTrue(ended - current >= (0.9 * waitMili));
	}

}

package com.ctrip.xpipe.utils;


import com.ctrip.xpipe.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

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

			Assert.assertTrue(ended - current <= 20);
		}

		long current = System.currentTimeMillis();
		offsetNotifier.await(initPos + 1, waitMili);
		long ended = System.currentTimeMillis();
		
		Assert.assertTrue(ended - current >= (0.9 * waitMili));
	}
	
	@Test
	public void testWaitInterrupt() throws IOException, TimeoutException {

		final AtomicBoolean interrupted = new AtomicBoolean(false);
		
		Thread t = new Thread(new Runnable() {
			
			@Override
			public void run() {
				try {
					logger.info("[run][begin wait]");
					offsetNotifier.await(initPos+1);
				} catch (InterruptedException e) {
					logger.error("[]", e);
					interrupted.set(true);
				}finally{
					logger.info("[run][end  wait]");
				}
			}
		});
		t.start();
		sleep(10);
		t.interrupt();
		
		waitConditionUntilTimeOut(() -> interrupted.get());
	}

}

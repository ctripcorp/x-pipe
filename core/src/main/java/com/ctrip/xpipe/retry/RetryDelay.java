package com.ctrip.xpipe.retry;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wenchao.meng
 *
 * Jul 9, 2016
 */
public class RetryDelay extends AbstractRetryWait{
	
	private AtomicInteger  count = new AtomicInteger(0);
	private int delayBaseMilli;
	
	public RetryDelay(int delayBaseMilli){
		this.delayBaseMilli = delayBaseMilli;
	}

	@Override
	public int retryWaitMilli() {
		
		int current = count.incrementAndGet();
		
		return current * delayBaseMilli;
	}

}

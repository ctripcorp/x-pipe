package com.ctrip.xpipe.retry;


import com.ctrip.xpipe.api.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wenchao.meng
 *
 * Jul 9, 2016
 */
public abstract class AbstractRetryPolicy implements RetryPolicy{

	protected Logger logger = LoggerFactory.getLogger(getClass());

	private AtomicInteger  count = new AtomicInteger(0);
	
	private int waitTimeOutMilli = 3600000;

	@Override
	public int retryWaitMilli() {
		try {
			return retryWaitMilli(false);
		} catch (InterruptedException e) {
			logger.info("[retryWaitMilli][impossible here]", e);
			throw new IllegalStateException("[retryWaitMilli][impossible here]");
		}
	}

	@Override
	public boolean retry(Throwable th) {
		return true;
	}
	
	@Override
	public int waitTimeoutMilli() {
		return waitTimeOutMilli;
	}

	@Override
	public int retryWaitMilli(boolean sleep) throws InterruptedException {
		
		int current = count.incrementAndGet();
		int sleepTime = getSleepTime(current);
		if(!sleep){
			return sleepTime;
		}
		
		try {
			TimeUnit.MILLISECONDS.sleep(sleepTime);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw e;
		}
		return sleepTime;
	}

	
	@Override
	public int getRetryTimes() {
		return count.get();
	}
	
	protected abstract int getSleepTime(int currentRetryTime);

	
	@Override
	public boolean timeoutCancel() {
		return false;
	}
	
}

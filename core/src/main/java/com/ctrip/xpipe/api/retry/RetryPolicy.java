package com.ctrip.xpipe.api.retry;

/**
 * @author wenchao.meng
 *
 * Jul 9, 2016
 */
public interface RetryPolicy {

	int retryWaitMilli();

	int retryWaitMilli(boolean sleep) throws InterruptedException;
	
	boolean retry(Throwable e);
	
	int waitTimeoutMilli();

	int getRetryTimes();
	
	boolean timeoutCancel();
}

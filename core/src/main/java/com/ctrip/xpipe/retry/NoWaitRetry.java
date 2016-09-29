package com.ctrip.xpipe.retry;


/**
 * @author wenchao.meng
 *
 * Jul 9, 2016
 */
public class NoWaitRetry extends AbstractRetryPolicy{

	@Override
	protected int getSleepTime(int currentRetryTime) {
		return 0;
	}



}

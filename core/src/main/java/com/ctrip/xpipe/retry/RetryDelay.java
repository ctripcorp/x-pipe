package com.ctrip.xpipe.retry;


/**
 * @author wenchao.meng
 *
 * Jul 9, 2016
 */
public class RetryDelay extends AbstractRetryPolicy{
	
	private int delayBaseMilli;
	
	public RetryDelay(int delayBaseMilli){
		this.delayBaseMilli = delayBaseMilli;
	}

	@Override
	protected int getSleepTime(int currentRetryTime) {
		return currentRetryTime * delayBaseMilli;
	}


}

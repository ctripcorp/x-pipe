package com.ctrip.xpipe.retry;


/**
 * @author wenchao.meng
 *
 * Jul 9, 2016
 */
public class NoWaitRetry extends AbstractRetryWait{

	@Override
	public int retryWaitMilli() {
		return 0;
	}


}

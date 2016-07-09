package com.ctrip.xpipe.retry;


/**
 * @author wenchao.meng
 *
 * Jul 9, 2016
 */
public class NoRetry extends RetryNTimes{
	public NoRetry() {
		super(0);
	}

}

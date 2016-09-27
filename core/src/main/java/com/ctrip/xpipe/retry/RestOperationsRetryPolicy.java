package com.ctrip.xpipe.retry;

import org.springframework.web.client.ResourceAccessException;

import com.ctrip.xpipe.api.retry.RetryPolicy;

/**
 * @author shyin
 *
 *         Sep 27, 2016
 */
public class RestOperationsRetryPolicy extends AbstractRetryPolicy implements RetryPolicy {

	private int retryInterval;

	public RestOperationsRetryPolicy() {
		this(10);
	}

	public RestOperationsRetryPolicy(int retryInterval) {
		this.retryInterval = retryInterval;
	}

	@Override
	public boolean retry(Throwable e) {
		return (e instanceof ResourceAccessException) ? true : false;
	}

	@Override
	protected int getSleepTime(int currentRetryTime) {
		return (retryInterval <= 0) ? 0 : retryInterval;
	}

}

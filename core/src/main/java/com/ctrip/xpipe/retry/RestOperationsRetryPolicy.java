package com.ctrip.xpipe.retry;

import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;

import com.ctrip.xpipe.api.retry.RetryPolicy;

/**
 * @author shyin
 *
 *         Sep 20, 2016
 */
public class RestOperationsRetryPolicy extends AbstractRetryPolicy implements RetryPolicy {

	private int retryInterval;
	
	public RestOperationsRetryPolicy() {
		this(5);
	}

	public RestOperationsRetryPolicy(int retryInterval) {
		this.retryInterval = retryInterval;
	}

	@Override
	public boolean retry(Throwable e) {
		if(e instanceof ResourceAccessException || e instanceof HttpServerErrorException) {
			return true;
		}
		return false;
	}

	@Override
	protected int getSleepTime(int currentRetryTime) {
		return (retryInterval <= 0) ? 0 : retryInterval;
	}

}

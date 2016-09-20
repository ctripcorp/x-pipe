package com.ctrip.xpipe.retry;

import com.ctrip.xpipe.api.retry.RetryPolicy;

/**
 * @author shyin
 *
 *         Sep 20, 2016
 */
public class RestOperationsRetryPolicyFactory implements RetryPolicyFactory {
	private int retryInterval;

	public RestOperationsRetryPolicyFactory(int retryInterval) {
		this.retryInterval = retryInterval;
	}

	@Override
	public RetryPolicy create() {
		return new RestOperationsRetryPolicy(retryInterval);
	}

}

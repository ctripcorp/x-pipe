package com.ctrip.xpipe.retry;

import com.ctrip.xpipe.api.retry.RetryPolicy;

/**
 * @author shyin
 *
 *         Sep 20, 2016
 */
public class RestOperationsRetryPolicyFactory extends AbstractRetryPolicyFactory {

	public RestOperationsRetryPolicyFactory(String[] args) {
		super(args);
	}

	@Override
	public RetryPolicy createRetryPolicy() {
		int retryInterval = Integer.valueOf(args[0]);
		int waitTimeoutMilli = Integer.valueOf(args[1]);
		return new RestOperationsRetryPolicy(retryInterval, waitTimeoutMilli);
	}

}

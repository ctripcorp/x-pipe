package com.ctrip.xpipe.retry;

/**
 * @author shyin
 *
 *         Sep 21, 2016
 */
public class RetryPolicyFactories {
	public static RetryPolicyFactory newRestOperationsRetryPolicyFactory() {
		return newRestOperationsRetryPolicyFactory(2);
	}

	public static RetryPolicyFactory newRestOperationsRetryPolicyFactory(int retryInterval) {
		return new RestOperationsRetryPolicyFactory(retryInterval);
	}
}

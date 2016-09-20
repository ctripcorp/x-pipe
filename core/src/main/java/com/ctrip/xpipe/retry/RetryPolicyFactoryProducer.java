package com.ctrip.xpipe.retry;

/**
 * @author shyin
 *
 *         Sep 20, 2016
 */
public class RetryPolicyFactoryProducer {
	public static AbstractRetryPolicyFactory getRetryPolicyFactory(RetryPolicyFactoryType type, String... args) {
		if (type.equals(RetryPolicyFactoryType.REST_OPERATIONS_RETRY_POLICY)) {
			return new RestOperationsRetryPolicyFactory(args);
		}
		return new RestOperationsRetryPolicyFactory(args);
	}
}

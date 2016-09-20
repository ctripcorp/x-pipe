package com.ctrip.xpipe.retry;

import com.ctrip.xpipe.api.retry.RetryPolicy;

/**
 * @author shyin
 *
 *         Sep 20, 2016
 */
public abstract class AbstractRetryPolicyFactory {
	protected String[] args;

	public AbstractRetryPolicyFactory(String[] args) {
		this.args = args;
	}

	public abstract RetryPolicy createRetryPolicy();
}

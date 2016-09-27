package com.ctrip.xpipe.retry;

import com.ctrip.xpipe.api.retry.RetryPolicy;

/**
 * @author shyin
 *
 *         Sep 27, 2016
 */
public interface RetryPolicyFactory {
	RetryPolicy create();
}

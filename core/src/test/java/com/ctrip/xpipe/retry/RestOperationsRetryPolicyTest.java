package com.ctrip.xpipe.retry;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.springframework.web.client.ResourceAccessException;

import com.ctrip.xpipe.AbstractTest;

/**
 * @author shyin
 *
 *         Sep 20, 2016
 */
public class RestOperationsRetryPolicyTest extends AbstractTest {
	private RestOperationsRetryPolicy policy;

	@Before
	public void setUp() {
		policy = new RestOperationsRetryPolicy();
	}

	@Test
	public void testCreate() {
		assertNotNull(policy);
		assertEquals(0, policy.getRetryTimes());
		assertEquals(300, policy.waitTimeoutMilli());
		assertEquals(false, policy.timeoutCancel());
	}

	@Test
	public void testRetryWait() throws InterruptedException {
		assertEquals(0, policy.retryWaitMilli());
		int sleepTime = 0;
		while (policy.retry(new ResourceAccessException(null))) {
			sleepTime = policy.retryWaitMilli(true);
		}
		assertEquals(300, sleepTime);
	}

	@Test
	public void testRetryJudgement() throws InterruptedException {
		assertEquals(true, policy.retry(new ResourceAccessException(null)));
		assertEquals(false, policy.retry(new IOException()));
		while (policy.retry(new ResourceAccessException(null))) {
			policy.retryWaitMilli(true);
		}
		assertEquals(false, policy.retry(new ResourceAccessException(null)));
	}

}

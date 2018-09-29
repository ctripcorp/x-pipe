package com.ctrip.xpipe.retry;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Before;
import org.junit.Test;
import org.springframework.web.client.ResourceAccessException;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
		assertEquals(3600000, policy.waitTimeoutMilli());
		assertEquals(false, policy.timeoutCancel());
	}

	@Test
	public void testRetryWait() throws InterruptedException {
		assertEquals(5, policy.retryWaitMilli());
		assertEquals(5, policy.getSleepTime(0));
	}

	@Test
	public void testRetryJudgement() throws InterruptedException {
		assertEquals(true, policy.retry(new ResourceAccessException(null)));
		assertEquals(false, policy.retry(new IOException()));
	}

}

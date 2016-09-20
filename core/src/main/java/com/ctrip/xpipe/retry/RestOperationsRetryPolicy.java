package com.ctrip.xpipe.retry;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.ResourceAccessException;

import com.ctrip.xpipe.api.retry.RetryPolicy;

/**
 * @author shyin
 *
 *         Sep 20, 2016
 */
public class RestOperationsRetryPolicy implements RetryPolicy {

	protected Logger logger = LoggerFactory.getLogger(getClass());

	private AtomicInteger count = new AtomicInteger(0);

	private int retryInterval;
	
	private int waitTimeOutMilli;

	public RestOperationsRetryPolicy() {
		this(10, 300);
	}

	public RestOperationsRetryPolicy(int retryInterval, int waitTimeOutMilli) {
		this.retryInterval = retryInterval;
		this.waitTimeOutMilli = waitTimeOutMilli;
	}

	@Override
	public int retryWaitMilli() {
		try {
			return retryWaitMilli(false);
		} catch (InterruptedException e) {
			logger.info("[retryWaitMilli][impossible here]", e);
			throw new IllegalStateException("[retryWaitMilli][impossible here]");
		}
	}

	@Override
	public int retryWaitMilli(boolean sleep) throws InterruptedException {
		if (!sleep) {
			return getSleepTime();
		}

		count.incrementAndGet();
		try {
			TimeUnit.MILLISECONDS.sleep(getInterval());
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw e;
		}
		return getSleepTime();
	}

	@Override
	public boolean retry(Throwable e) {
		if (waitTimeoutMilli() > 0 && waitTimeoutMilli() > getSleepTime()) {
			if (e instanceof ResourceAccessException) {
				return true;
			}
		}
		return false;
	}

	private int getInterval() {
		return (retryInterval <= 0) ? 0 : retryInterval;
	}

	private int getSleepTime() {
		return getRetryTimes() * getInterval();
	}

	@Override
	public int waitTimeoutMilli() {
		return (waitTimeOutMilli <= 0) ? 0 : waitTimeOutMilli;
	}

	@Override
	public int getRetryTimes() {
		return count.get();
	}

	@Override
	public boolean timeoutCancel() {
		return false;
	}

}

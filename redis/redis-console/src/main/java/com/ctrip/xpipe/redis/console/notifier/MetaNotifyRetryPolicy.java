package com.ctrip.xpipe.redis.console.notifier;

import com.ctrip.xpipe.api.retry.RetryPolicy;
import com.ctrip.xpipe.retry.AbstractRetryPolicy;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;

import java.util.concurrent.TimeoutException;

/**
 * @author shyin
 *
 * Oct 18, 2016
 */
public class MetaNotifyRetryPolicy extends AbstractRetryPolicy implements RetryPolicy{

	private int retryInterval;
	
	public MetaNotifyRetryPolicy() {
		this(100);
	}
	
	public MetaNotifyRetryPolicy(int interval) {
		this.retryInterval = interval;
	}
	
	@Override
	public int waitTimeoutMilli() {
		return 2000;
	}
	
	@Override
	public boolean retry(Throwable e) {
		if(e instanceof ResourceAccessException || e instanceof HttpServerErrorException || e instanceof TimeoutException) {
			return true;
		}
		return false;
	}
	
	@Override
	protected int getSleepTime(int currentRetryTime) {
		return (retryInterval <= 0) ? 0 : retryInterval;
	}

}

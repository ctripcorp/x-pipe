package com.ctrip.xpipe.redis.console.health;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author marsqing
 *
 *         Dec 6, 2016 5:35:48 PM
 */
public class BaseInstanceResult<T> {

	protected AtomicLong rcvNanoTime = new AtomicLong();
	protected T context;

	public boolean isDone() {
		return rcvNanoTime.get() > 0;
	}

	public void done(long rcvNanoTime, T context) {
		if (rcvNanoTime > 0) {
			this.rcvNanoTime.set(rcvNanoTime);
			this.context = context;
		}
	}

	public long calculateDelay(long publishNanoTime) {
		return rcvNanoTime.get() - publishNanoTime;
	}

	public AtomicLong getRcvNanoTime() {
		return rcvNanoTime;
	}

	public T getContext() {
		return context;
	}

	public void setContext(T context) {
		this.context = context;
	}


	@Override
	public String toString() {
		return String.format("context:%s", context);
	}
}

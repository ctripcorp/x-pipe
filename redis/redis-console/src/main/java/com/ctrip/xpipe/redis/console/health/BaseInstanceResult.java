package com.ctrip.xpipe.redis.console.health;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author marsqing
 *
 *         Dec 6, 2016 5:35:48 PM
 */
public class BaseInstanceResult<T> {

	protected AtomicLong rcvNanoTime = new AtomicLong();
	protected T context;
	protected Throwable failReason;

	protected Logger logger = LoggerFactory.getLogger(getClass());

	public boolean isDone() {
		return rcvNanoTime.get() > 0;
	}

	public void success(long rcvNanoTime, T context) {

		if (rcvNanoTime <= 0) {
			throw new IllegalArgumentException("argument error:" + rcvNanoTime);
		}

		if (rcvNanoTime > 0) {
			this.rcvNanoTime.set(rcvNanoTime);
			this.context = context;
		}
	}

	public void fail(long rcvNanoTime, Throwable th){

		if (rcvNanoTime <= 0 || th == null) {
			throw new IllegalArgumentException("argument error:" + rcvNanoTime + th);
		}

		this.rcvNanoTime.set(rcvNanoTime);
		this.failReason = th;
	}

	public boolean isSuccess(){
		return !isFail();
	}

	public boolean isFail(){
		return this.failReason != null;
	}

	public Throwable getFailReason() {
		return failReason;
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

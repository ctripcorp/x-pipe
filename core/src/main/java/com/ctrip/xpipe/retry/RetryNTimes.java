package com.ctrip.xpipe.retry;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.retry.RetryPolicy;
import com.ctrip.xpipe.api.retry.RetryTemplate;

/**
 * @author wenchao.meng
 *
 *         Jul 9, 2016
 */
public class RetryNTimes<V> extends AbstractRetryTemplate<V> {

	private int n;
	private RetryPolicy retryPolicy;
	private AtomicBoolean destroyed = new AtomicBoolean(false);

	public RetryNTimes(int n, int delayBaseMilli) {
		this(n, new RetryDelay(delayBaseMilli));
	}

	public RetryNTimes(int n) {
		this(n, new NoWaitRetry());
	}

	public RetryNTimes(int n, RetryPolicy retryPolicy) {
		this.n = n;
		this.retryPolicy = retryPolicy;
	}

	public static <V> RetryTemplate<V> retryForEver(RetryPolicy retryPolicy) {
		return new RetryNTimes<>(-1, retryPolicy);
	}

	public static <V> RetryTemplate<V> noRetry() {
		return new RetryNTimes<>(0);
	}

	@Override
	public V execute(Command<V> command) throws Exception {

		for (int i = 0; n == -1 || i <= n; i++) {

			if(destroyed.get()){
				logger.info("[execute][destroyed return null]{}", this);
				return null;
			}

			if (i >= 1) {
				logger.info("[execute][retry]{}, {}", i, command);
				retryPolicy.retryWaitMilli(true);
			}

			if(destroyed.get()){
				logger.info("[execute][destroyed return null]{}", this);
				return null;
			}

			try {
				logger.debug("[execute]{}, {}", i, command);
				CommandFuture<V> future = command.execute();
				if(future == null){
					return null;
				}
				return future.get(retryPolicy.waitTimeoutMilli(), TimeUnit.MILLISECONDS);
			} catch (Exception e) {
				logger.error(String.format("cmd:%s, message:%s", command, e.getMessage()), e);
				Exception originalException = getOriginalException(e);
				if (i == n || !retryPolicy.retry(originalException)) {
					throw originalException;
				}
			}
			command.reset();
		}
		return null;
	}

	protected static Exception getOriginalException(Throwable e) {
		if (e instanceof ExecutionException) {
			return (Exception) ((null == e.getCause())?e : getOriginalException(e.getCause()));
		}
		if (e instanceof InvocationTargetException) {
			return (Exception) ((null == e.getCause())?e : getOriginalException(e.getCause()));
		}
		return (Exception) e;

	}

	@Override
	public void destroy() throws Exception {
		logger.info("[destroy]{}", this);
		destroyed.set(true);
	}
}

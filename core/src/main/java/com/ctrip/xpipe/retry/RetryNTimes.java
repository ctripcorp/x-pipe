package com.ctrip.xpipe.retry;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.ctrip.xpipe.api.command.Command;
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

			if (i >= 1) {
				logger.info("[execute][retry]{}, {}", i, command);
				retryPolicy.retryWaitMilli(true);
			}

			try {
				logger.debug("[execute]{}, {}", i, command);
				return command.execute().get(retryPolicy.waitTimeoutMilli(), TimeUnit.MILLISECONDS);
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
}

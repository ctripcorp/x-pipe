package com.ctrip.xpipe.redis.console.job.retry;

import com.ctrip.xpipe.api.retry.RetryPolicy;

import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Mar 02, 2018
 */
public interface RetryCondition<V> extends RetryPolicy {

    boolean isSatisfied(V v);

    boolean isExceptionExpected(Throwable th);

    abstract class AbstractRetryCondition<V> implements RetryCondition<V> {
        @Override
        public int retryWaitMilli() {
            return (int) TimeUnit.SECONDS.toMillis(1);
        }

        @Override
        public int retryWaitMilli(boolean sleep) throws InterruptedException {
            return (int) TimeUnit.SECONDS.toMillis(1);
        }

        @Override
        public boolean retry(Throwable e) {
            return isExceptionExpected(e);
        }

        @Override
        public int waitTimeoutMilli() {
            return (int) TimeUnit.SECONDS.toMillis(2);
        }

        @Override
        public int getRetryTimes() {
            return 0;
        }

        @Override
        public boolean timeoutCancel() {
            return false;
        }
    }

    class DefaultRetryCondition<V> extends AbstractRetryCondition<V> {

        @Override
        public boolean isSatisfied(V v) {
            return true;
        }

        @Override
        public boolean isExceptionExpected(Throwable th) {
            return false;
        }


    }
}

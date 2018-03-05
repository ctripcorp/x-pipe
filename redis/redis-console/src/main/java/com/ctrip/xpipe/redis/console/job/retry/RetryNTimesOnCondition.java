package com.ctrip.xpipe.redis.console.job.retry;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.console.exception.ServerException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author chen.zhu
 * <p>
 * Mar 02, 2018
 */
public class RetryNTimesOnCondition<V> extends AbstractRetryOnCondition<V> {

    private int retryTimes;

    private AtomicInteger retryTimesCounter = new AtomicInteger(0);

    public RetryNTimesOnCondition(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public RetryNTimesOnCondition(RetryCondition<V> retryCondition, int retryTimes) {
        super(retryCondition);
        this.retryTimes = retryTimes;
    }

    @Override
    protected V doExecute(Command<V> command) throws InterruptedException, ExecutionException, TimeoutException {
        if(retryTimesCounter.getAndIncrement() > retryTimes) {
            throw new ServerException("Retry times out");
        }
        CommandFuture<V> future = command.execute();
        if(future == null){
            return null;
        }
        return future.get(retryCondition.waitTimeoutMilli(), TimeUnit.MILLISECONDS);
    }
}

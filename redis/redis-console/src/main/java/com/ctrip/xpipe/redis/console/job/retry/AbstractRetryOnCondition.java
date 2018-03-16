package com.ctrip.xpipe.redis.console.job.retry;

import com.ctrip.xpipe.api.command.Command;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * @author chen.zhu
 * <p>
 * Mar 02, 2018
 */
public abstract class AbstractRetryOnCondition<V> extends AbstractConsoleRetryTemplate<V> {

    protected RetryCondition<V> retryCondition;

    protected AbstractRetryOnCondition() {
        this(new RetryCondition.DefaultRetryCondition<>());
    }

    protected AbstractRetryOnCondition(RetryCondition<V> retryCondition) {
        this.retryCondition = retryCondition;
    }

    @Override
    public V execute(Command<V> command) throws Exception {
        V result;
        try {
            if(isTerminated()) {
                logger.warn("[execute] task has already been terminated. task info: {}", command);
                return null;
            }
            logger.debug("[execute] {}", command);

            result = doExecute(command);

            if(!retryCondition.isSatisfied(result)) {
                logger.info("[execute] Result: {}, not satisfy target, re-execute command", result);
                return execute(command);
            }
            return result;
        } catch (Exception e) {
            logger.error(String.format("cmd:%s, message:%s", command, e.getMessage()), e);

            if(retryCondition.isExceptionExpected(e)) {
                execute(command);
            } else {
                logger.error("[execute] Unexpected Exception occurred, command force terminate");
                destroy();
                throw e;
            }
        }
        command.reset();
        return null;
    }

    protected abstract V doExecute(Command<V> command) throws InterruptedException, ExecutionException, TimeoutException;
}

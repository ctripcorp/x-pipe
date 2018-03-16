package com.ctrip.xpipe.redis.console.job.retry;

import com.ctrip.xpipe.retry.AbstractRetryTemplate;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author chen.zhu
 * <p>
 * Mar 02, 2018
 */
public abstract class AbstractConsoleRetryTemplate<V> extends AbstractRetryTemplate<V> {

    protected AtomicBoolean isTerminated = new AtomicBoolean(false);

    @Override
    public void destroy() throws Exception {
        logger.info("[destroy]{}", this);
        isTerminated.set(true);
    }

    protected boolean isTerminated() {
        return isTerminated.get();
    }
}

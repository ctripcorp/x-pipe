package com.ctrip.xpipe.redis.console.healthcheck.crossdc;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * @author chen.zhu
 * <p>
 * Oct 11, 2018
 */
public abstract class SafeLoop<T> {

    private List<T> list;

    private Executor executors;

    public SafeLoop(Collection<T> list) {
        this(MoreExecutors.directExecutor(), list);
    }

    public SafeLoop(Executor executors, Collection<T> src) {
        this.executors = executors;
        this.list = Lists.newLinkedList(src);
    }

    public void run() {
        for(T t : list) {
            executors.execute(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() throws Exception {
                    try {
                        doRun0(t);
                    } catch (Exception e) {
                        logger.error("[SafeLoop][{}]", getInfo(t), e);
                    }
                }
            });

        }
    }

    protected abstract void doRun0(T t) throws Exception;

    String getInfo(T t) {
        return t.toString();
    }
}

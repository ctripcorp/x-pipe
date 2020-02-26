package com.ctrip.xpipe.redis.console.election;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.observer.AbstractObservable;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

import java.util.concurrent.*;

public abstract class AbstractPeriodicElectionAction extends AbstractObservable implements ElectionAction, Startable, Stoppable {

    private ScheduledFuture future;

    private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("CrossDcServer"));

    @Override
    public void start() throws Exception {
        stop();
        elect();
    }

    public void stop() throws Exception {
        if (null != future && !future.isDone()) future.cancel(false);
    }

    protected void elect() {
        if (shouldElect()) {
            beforeElect();
            doElect();
            afterElect();
        }

        future = scheduled.schedule(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() throws Exception {
                elect();
            }

        }, getElectIntervalMillSecond(), TimeUnit.MILLISECONDS);
    }

    protected abstract void doElect();

    protected abstract boolean shouldElect();

    protected abstract void beforeElect();

    protected abstract void afterElect();

    protected abstract long getElectIntervalMillSecond();

}

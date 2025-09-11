package com.ctrip.xpipe.redis.console.election;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.observer.AbstractObservable;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractPeriodicElectionAction extends AbstractObservable implements ElectionAction, Startable, Stoppable {

    private ScheduledFuture<?> future;

    private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create(getElectionName()));

    private AtomicBoolean isStarted = new AtomicBoolean(false);

    @Override
    public void start() {
        if (isStarted.compareAndSet(false, true)) {
            elect();
        }
    }

    @Override
    public void stop() {
        if (isStarted.compareAndSet(true, false) && null != future && !future.isDone()) {
            future.cancel(false);
            future = null;
        }
    }

    public void resetNextElection(long nextElectionDelay) {
        if (!isStarted.get()) {
            logger.info("[resetNextElection] should start first");
            return;
        }

        setNextElection(nextElectionDelay);
    }

    protected void elect() {
        if (shouldElect()) {
            beforeElect();
            doElect();
            afterElect();
        }

        setNextElection(getElectIntervalMillSecond());
    }

    private synchronized void setNextElection(long nextElectionDelay) {
        if (!isStarted.get()) return;
        if (null != future && !future.isDone()) {
            future.cancel(false);
        }

        future = scheduled.schedule(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() {
                elect();
            }

        }, nextElectionDelay, TimeUnit.MILLISECONDS);
    }

    protected abstract void doElect();

    protected abstract boolean shouldElect();

    protected abstract void beforeElect();

    protected abstract void afterElect();

    protected abstract long getElectIntervalMillSecond();

    protected abstract String getElectionName();

}

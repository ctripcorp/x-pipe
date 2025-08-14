package com.ctrip.xpipe.utils.job;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * @author lishanglin
 * date 2021/3/16
 */
public class DynamicDelayPeriodTask extends AbstractStartStoppable implements Startable, Stoppable {

    private String name;

    private Runnable innerTask;

    private LongSupplier delaySupplier;

    private LongSupplier initDelaySupplier;

    private ScheduledExecutorService scheduled;

    private ScheduledFuture<?> future;

    public DynamicDelayPeriodTask(String name, Runnable task, LongSupplier initDelaySupplier, LongSupplier delaySupplier, ScheduledExecutorService scheduled) {
        this.name = name;
        this.innerTask = task;
        this.delaySupplier = delaySupplier;
        this.initDelaySupplier = initDelaySupplier;
        this.scheduled = scheduled;
    }

    public DynamicDelayPeriodTask(String name, Runnable task, LongSupplier delaySupplier, ScheduledExecutorService scheduled) {
        this(name, task, null, delaySupplier, scheduled);
    }

    @Override
    protected void doStart() throws Exception {
        scheduled.schedule(this::doRun, initDelaySupplier == null ? 0 : initDelaySupplier.getAsLong(), TimeUnit.MILLISECONDS);
    }

    @Override
    protected void doStop() throws Exception {
        if (null != future && !future.isDone()) future.cancel(false);
        future = null;
    }

    private synchronized void doRun() {
        if (!isStarted()) return;

        try {
            logger.debug("[doRun][{}] run", name);
            this.innerTask.run();
        } catch (Throwable th) {
            logger.info("[doRun][{}] fail", name, th);
        } finally {
            if (isStarted()) {
                long delay = delaySupplier.getAsLong();
                logger.debug("[doRun][{}] delay {}", name, delay);
                future = scheduled.schedule(this::doRun, delay, TimeUnit.MILLISECONDS);
            }
        }
    }

}

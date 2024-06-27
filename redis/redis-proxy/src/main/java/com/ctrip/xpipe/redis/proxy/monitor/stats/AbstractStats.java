package com.ctrip.xpipe.redis.proxy.monitor.stats;

import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.session.state.SessionEstablished;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Oct 31, 2018
 */
public abstract class AbstractStats extends AbstractStartStoppable implements Runnable {

    private ScheduledExecutorService scheduled;

    private static final int ONE_SEC = 1000;

    private Session session;

    private DynamicDelayPeriodTask task;

    public AbstractStats(Session session, ScheduledExecutorService scheduled) {
        this.session = session;
        this.task = new DynamicDelayPeriodTask(String.format("session:%d", session.getSessionId()), this, this::getTaskDelayMilli, scheduled);
        this.scheduled = scheduled;
    }

    protected long getTaskDelayMilli() {
        if (session.getSessionState() instanceof SessionEstablished) {
            return getCheckIntervalMilli();
        } else {
            // hang for session established
            return getCheckHangMilli();
        }
    }

    @Override
    public void run() {
        if (session.getSessionState() instanceof SessionEstablished) {
            doTask();
        }
    }

    @Override
    protected void doStart() throws Exception {
        task.start();
    }

    @Override
    protected void doStop() throws Exception {
        task.stop();
    }

    protected Session getSession() {
        return session;
    }

    protected ScheduledExecutorService getScheduled() {
        return scheduled;
    }

    protected int getCheckIntervalMilli() {
        return ONE_SEC;
    }

    protected int getCheckHangMilli() {
        return ONE_SEC * 60;
    }

    protected abstract void doTask();
}

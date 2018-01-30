package com.ctrip.xpipe.concurrent;

import com.ctrip.xpipe.api.monitor.EventMonitor;

/**
 * @author wenchao.meng
 *         <p>
 *         Jan 30, 2018
 */
public class LongTimeAlertTask extends AbstractExceptionLogTask {

    private Runnable task;
    private long alertMilli;

    public LongTimeAlertTask(Runnable task, long alertMilli){
        this.task = task;
        this.alertMilli = alertMilli;
    }

    @Override
    protected void doRun() throws Exception {

        long begin = System.currentTimeMillis();
        task.run();
        long end = System.currentTimeMillis();
        long duration = end - begin;
        if( duration >= alertMilli){
            logger.warn("[doRun]{} ms, {}", duration, task);
            EventMonitor.DEFAULT.logAlertEvent(String.format("%d ms, %s", duration, task));
        }
    }
}

package com.ctrip.xpipe.redis.console.alert.sender.email.listener;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.THREAD_POOL_TIME_OUT;

/**
 * @author chen.zhu
 * <p>
 * Mar 26, 2018
 */
public class EmailSentCounter extends AbstractEmailSenderCallback {

    private AtomicInteger total = new AtomicInteger();

    private AtomicInteger success = new AtomicInteger();

    private AtomicInteger fail = new AtomicInteger();

    private Lock lock = new ReentrantLock();

    private ScheduledExecutorService scheduled;

    public EmailSentCounter() {
        scheduled = MoreExecutors.getExitingScheduledExecutorService(
                new ScheduledThreadPoolExecutor(1, XpipeThreadFactory.create(getClass().getSimpleName() + "-")),
                THREAD_POOL_TIME_OUT, TimeUnit.SECONDS
        );
        start(scheduled);
    }

    private void start(ScheduledExecutorService scheduled) {
        scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                scheduledTask();
            }
        }, 1, 60, TimeUnit.MINUTES);
    }

    protected void scheduledTask() {
        logger.info("[scheduledTask] start retrieving info");
        int totalCount, successCount, failCount;
        try {
            lock.lock();
            totalCount = total.getAndSet(0);
            successCount = success.getAndSet(0);
            failCount = fail.getAndSet(0);
        } finally {
            lock.unlock();
        }
        logger.info("[scheduledTask] scheduled report, total email count: {}", totalCount);
        EventMonitor.DEFAULT.logEvent(EMAIL_SERVICE_CAT_TYPE,
                String.format("total sent out emails in an hour: %d", totalCount));
        EventMonitor.DEFAULT.logEvent(EMAIL_SERVICE_CAT_TYPE,
                String.format("success sent out emails in an hour: %d", successCount));
        EventMonitor.DEFAULT.logEvent(EMAIL_SERVICE_CAT_TYPE,
                String.format("failed sent out emails in an hour: %d", failCount));
    }

    @Override
    public void success() {
        try {
            lock.lock();
            success.getAndIncrement();
            total.getAndIncrement();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void fail(Throwable throwable) {
        try {
            lock.lock();
            total.getAndIncrement();
            fail.getAndIncrement();
        } finally {
            lock.unlock();
        }
    }

    public int getTotal() {
        return total.get();
    }

    public int getFailed() {
        return fail.get();
    }

    public int getSuccess() {
        return success.get();
    }

}

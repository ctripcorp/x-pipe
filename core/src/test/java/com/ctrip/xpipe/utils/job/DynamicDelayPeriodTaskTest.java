package com.ctrip.xpipe.utils.job;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author lishanglin
 * date 2021/3/17
 */
public class DynamicDelayPeriodTaskTest extends AbstractTest {

    private DynamicDelayPeriodTask task;

    private AtomicInteger runTimes = new AtomicInteger(0);

    private AtomicLong lastRunTime = new AtomicLong(0);

    private AtomicBoolean periodWrong = new AtomicBoolean(false);

    private long delta = 10;

    @Test
    public void testPeriodChange() throws Exception {
        task = new DynamicDelayPeriodTask("test", new TestPeriodChangeTask(), this::getNextDelay, scheduled);
        task.start();
        waitConditionUntilTimeOut(() -> runTimes.get() >= 3, 5000);
        Assert.assertFalse(periodWrong.get());
        task.stop();
        sleep((int) (getNextDelay() + delta));
        Assert.assertEquals(3, runTimes.get());
    }

    private long getNextDelay() {
        return 100 * runTimes.get();
    }

    private class TestPeriodChangeTask implements Runnable {

        @Override
        public void run() {
            long current = System.currentTimeMillis();
            if (lastRunTime.get() > 0 &&
                    (current - lastRunTime.get() < getNextDelay() || current -lastRunTime.get() > getNextDelay() + delta)) {
                periodWrong.set(true);
            }

            lastRunTime.set(current);
            runTimes.incrementAndGet();
        }
    }

}

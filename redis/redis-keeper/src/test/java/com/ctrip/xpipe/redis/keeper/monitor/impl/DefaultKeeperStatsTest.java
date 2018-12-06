package com.ctrip.xpipe.redis.keeper.monitor.impl;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperStats;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author chen.zhu
 * <p>
 * Oct 22, 2018
 */
public class DefaultKeeperStatsTest extends AbstractTest {

    private KeeperStats keeperStats;

    private static final int FIXED_INCREASING_BYTES = 1000;

    @Before
    public void beforeDefaultKeeperStatsTest() {
        keeperStats = new DefaultKeeperStats(scheduled);
    }

    @Ignore
    @Test
    public void testGetInputInstantaneousKBPS() throws Exception {
        int n = 10;
        keeperStats.start();
        for(int i = 0; i < 10; i++) {
            keeperStats.increaseInputBytes(1000);
            sleep(1000);
        }
        long stats = keeperStats.getInputInstantaneousBPS();
        logger.info("{}", stats);
    }

    @Test
    public void testIncreaseInputBytes() {
        int n = 10;
        for(int i = 0; i < n; i++) {
            keeperStats.increaseInputBytes(FIXED_INCREASING_BYTES);
        }
        Assert.assertEquals(n * FIXED_INCREASING_BYTES, keeperStats.getInputBytes());
    }

    @Test
    public void testIncreaseOutputBytes() {
        int n = 10;
        for(int i = 0; i < n; i++) {
            keeperStats.increaseOutputBytes(FIXED_INCREASING_BYTES);
        }
        Assert.assertEquals(n * FIXED_INCREASING_BYTES, keeperStats.getOutputBytes());
    }

    @Test
    public void testGetInputBytes() throws TimeoutException {
        int n = 10;
        for(int i = 0; i < n; i++) {
            executors.execute(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() {
                    keeperStats.increaseInputBytes(FIXED_INCREASING_BYTES);
                }
            });
        }
        waitConditionUntilTimeOut(()->((ThreadPoolExecutor)executors).getCompletedTaskCount() >= n, 1000);
        Assert.assertEquals(n * FIXED_INCREASING_BYTES, keeperStats.getInputBytes());
    }

    @Test
    public void testGetOutputBytes() throws TimeoutException {
        int n = 10;
        for(int i = 0; i < n; i++) {
            executors.execute(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() {
                    keeperStats.increaseOutputBytes(FIXED_INCREASING_BYTES);
                }
            });
        }
        waitConditionUntilTimeOut(()->((ThreadPoolExecutor)executors).getCompletedTaskCount() >= n, 1000);
        Assert.assertEquals(n * FIXED_INCREASING_BYTES, keeperStats.getOutputBytes());
    }
}
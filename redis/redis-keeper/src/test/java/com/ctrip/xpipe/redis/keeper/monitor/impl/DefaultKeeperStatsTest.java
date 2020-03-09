package com.ctrip.xpipe.redis.keeper.monitor.impl;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperStats;
import com.ctrip.xpipe.redis.keeper.monitor.PsyncFailReason;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

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
        keeperStats = new DefaultKeeperStats("shard", scheduled);
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

    @Test
    public void testLastFailReason() throws Exception {
        Assert.assertNull(keeperStats.getLastPsyncFailReason());
        keeperStats.setLastPsyncFailReason(null);
        Assert.assertNull(keeperStats.getLastPsyncFailReason());
        keeperStats.setLastPsyncFailReason(PsyncFailReason.TOKEN_LACK);
        Assert.assertEquals(PsyncFailReason.TOKEN_LACK, keeperStats.getLastPsyncFailReason());
    }

    @Test
    public void testSendFailCount() throws TimeoutException {
        int n = 10;
        for(int i = 0; i < n; i++) {
            executors.execute(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() {
                    keeperStats.increasePsyncSendFail();
                }
            });
        }
        waitConditionUntilTimeOut(()->((ThreadPoolExecutor)executors).getCompletedTaskCount() >= n, 1000);
        Assert.assertEquals(n, keeperStats.getPsyncSendFailCount());
    }

    //manul test
    @Test
    @Ignore
    public void testGetPeakInputBytes() throws Exception {
        int interval = 10;
        long n = TimeUnit.SECONDS.toMillis(1) / interval;
        keeperStats.start();
        sleep(100);
        for(int i = 0; i < n; i++) {
            keeperStats.increaseInputBytes(1000);
            sleep(interval);
        }

        waitConditionUntilTimeOut(()->(keeperStats.getPeakInputInstantaneousBPS()) > 0);
        long stats = keeperStats.getPeakInputInstantaneousBPS();
        sleep(1000);
        long stats2 = keeperStats.getPeakInputInstantaneousBPS();
        logger.info("[{}] v.s [{}]", stats, stats2);
        keeperStats.stop();
        Assert.assertEquals(stats, stats2);
    }

    //manul test
    @Test
    @Ignore
    public void testGetPeakOutputBytes() throws Exception {
        int interval = 10;
        long n = TimeUnit.SECONDS.toMillis(1) / interval;
        keeperStats.start();
        sleep(100);
        for(int i = 0; i < n; i++) {
            keeperStats.increaseOutputBytes(1000);
            sleep(interval);
        }

        waitConditionUntilTimeOut(()->(keeperStats.getPeakOutputInstantaneousBPS()) > 0);
        long stats = keeperStats.getPeakOutputInstantaneousBPS();
        sleep(1000);
        long stats2 = keeperStats.getPeakOutputInstantaneousBPS();
        logger.info("[{}] v.s [{}]", stats, stats2);
        keeperStats.stop();
        Assert.assertEquals(stats, stats2);
    }

    //manually test
    @Test
    @Ignore
    public void testGetPeakOutputBytesManually() throws Exception {
        int interval = 10;
        long n = TimeUnit.SECONDS.toMillis(60) / interval;
        keeperStats.start();
        sleep(100);
        for(int i = 0; i < n; i++) {
            keeperStats.increaseInputBytes(1000);
            sleep(interval);
            if(i % 100 == 0) {
                logger.info("[{}]", keeperStats.getPeakInputInstantaneousBPS());
            }
        }

        sleep(1000);
        logger.info("[{}]", keeperStats.getPeakInputInstantaneousBPS());
//        waitConditionUntilTimeOut(()->(keeperStats.getPeakOutputInstantaneousBPS()) > 0);
//        long stats = keeperStats.getPeakOutputInstantaneousBPS();
//        sleep(1000);
//        long stats2 = keeperStats.getPeakOutputInstantaneousBPS();
//        logger.info("[{}] v.s [{}]", stats, stats2);
        keeperStats.stop();
//        Assert.assertEquals(stats, stats2);
    }
}
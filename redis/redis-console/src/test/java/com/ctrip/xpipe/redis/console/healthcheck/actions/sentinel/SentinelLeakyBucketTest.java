package com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * May 19, 2020
 */
public class SentinelLeakyBucketTest extends AbstractTest {

    private SentinelLeakyBucket leakyBucket;

    @Mock
    private ConsoleConfig consoleConfig;

    @Before
    public void beforeSentinelLeakyBucketTest() {
        MockitoAnnotations.initMocks(this);
        leakyBucket = new SentinelLeakyBucket(consoleConfig, scheduled);
    }

    @Test
    public void testTryAcquireWithNotOpen() throws InterruptedException {
        when(consoleConfig.isSentinelRateLimitOpen()).thenReturn(false);
        int tasks = 100;
        CountDownLatch latch = new CountDownLatch(tasks);
        CyclicBarrier barrier = new CyclicBarrier(tasks);
        AtomicInteger counter = new AtomicInteger();
        for(int i = 0; i < tasks; i ++) {
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        barrier.await();
                    } catch (Exception ignore) {
                    }
                    if(leakyBucket.tryAcquire()) {
                        counter.incrementAndGet();
                    }
                    latch.countDown();
                }
            });
        }
        latch.await(1500, TimeUnit.MILLISECONDS);
        Assert.assertEquals(tasks, counter.get());
    }

    @Test
    public void testTryAcquireWithOpen() throws InterruptedException {
        when(consoleConfig.isSentinelRateLimitOpen()).thenReturn(true);
        when(consoleConfig.getSentinelRateLimitSize()).thenReturn(3);
        leakyBucket = new SentinelLeakyBucket(consoleConfig, scheduled);
        int tasks = 100;
        CountDownLatch latch = new CountDownLatch(tasks);
        CyclicBarrier barrier = new CyclicBarrier(tasks);
        AtomicInteger counter = new AtomicInteger();
        for(int i = 0; i < tasks; i ++) {
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        barrier.await();
                    } catch (Exception ignore) {
                    }
                    if(leakyBucket.tryAcquire()) {
                        counter.incrementAndGet();
                    }
                    latch.countDown();
                }
            });
        }
        latch.await(1500, TimeUnit.MILLISECONDS);
        Assert.assertEquals(leakyBucket.getTotalSize(), counter.get());
    }

    @Test
    public void testRelease() throws InterruptedException {
        when(consoleConfig.isSentinelRateLimitOpen()).thenReturn(true);
        when(consoleConfig.getSentinelRateLimitSize()).thenReturn(3);
        leakyBucket = new SentinelLeakyBucket(consoleConfig, scheduled);
        int tasks = 100;
        CountDownLatch latch = new CountDownLatch(tasks);
        CyclicBarrier barrier = new CyclicBarrier(tasks);
        AtomicInteger counter = new AtomicInteger();
        for(int i = 0; i < tasks; i ++) {
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        barrier.await();
                    } catch (Exception ignore) {
                    }
                    if(leakyBucket.tryAcquire()) {
                        counter.incrementAndGet();
                        leakyBucket.release();
                    }
                    latch.countDown();
                }
            });
        }
        latch.await(1500, TimeUnit.MILLISECONDS);
        Assert.assertTrue(leakyBucket.getTotalSize() < counter.get());
    }

    @Test
    public void testDelayRelease() {
        when(consoleConfig.isSentinelRateLimitOpen()).thenReturn(true);
        when(consoleConfig.getSentinelRateLimitSize()).thenReturn(3);
        leakyBucket = new SentinelLeakyBucket(consoleConfig, scheduled);
        Assert.assertTrue(leakyBucket.tryAcquire());
        Assert.assertTrue(leakyBucket.tryAcquire());
        Assert.assertTrue(leakyBucket.tryAcquire());
        leakyBucket.delayRelease(30, TimeUnit.MILLISECONDS);
        Assert.assertFalse(leakyBucket.tryAcquire());
        sleep(30);
        Assert.assertTrue(leakyBucket.tryAcquire());
    }

    @Test
    public void testResize() {
    }

    @Test
    public void testReferences() {
        when(consoleConfig.isSentinelRateLimitOpen()).thenReturn(true);
        when(consoleConfig.getSentinelRateLimitSize()).thenReturn(3);
        leakyBucket = new SentinelLeakyBucket(consoleConfig, scheduled);
        leakyBucket.tryAcquire();
        Assert.assertEquals(2, leakyBucket.references());
        leakyBucket.tryAcquire();
        Assert.assertEquals(1, leakyBucket.references());
    }

    @Test
    public void testGetTotalSize() {
        when(consoleConfig.isSentinelRateLimitOpen()).thenReturn(true);
        when(consoleConfig.getSentinelRateLimitSize()).thenReturn(3);
        leakyBucket = new SentinelLeakyBucket(consoleConfig, scheduled);
        Assert.assertEquals(3, leakyBucket.getTotalSize());
    }

    @Test
    public void testDoStart() throws Exception {
        SentinelLeakyBucket.PERIODIC_MILLI = 10;
        when(consoleConfig.isSentinelRateLimitOpen()).thenReturn(true);
        when(consoleConfig.getSentinelRateLimitSize()).thenReturn(3);
        leakyBucket = new SentinelLeakyBucket(consoleConfig, scheduled);
        leakyBucket.start();
        sleep(20);
        verify(consoleConfig, atLeast(2)).getSentinelRateLimitSize();
        leakyBucket.stop();
    }

    @Test
    public void testDoStop() {
    }
}
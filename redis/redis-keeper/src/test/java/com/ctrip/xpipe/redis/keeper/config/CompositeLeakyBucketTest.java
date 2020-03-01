package com.ctrip.xpipe.redis.keeper.config;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService;
import com.ctrip.xpipe.redis.keeper.container.KeeperContainerService;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Feb 25, 2020
 */
public class CompositeLeakyBucketTest extends AbstractTest {

    private CompositeLeakyBucket leakyBucket;

    @Mock
    private MetaServerKeeperService metaServerKeeperService;

    @Mock
    private KeeperContainerService keeperContainerService;

    @Before
    public void beforeCompositeLeakyBucketTest() {
        MockitoAnnotations.initMocks(this);
        when(keeperContainerService.list()).thenReturn(Lists.newArrayList());
        leakyBucket = new CompositeLeakyBucket(new TestKeeperConfig(), metaServerKeeperService, keeperContainerService);
    }

    @Test
    public void testTryAcquire() throws InterruptedException {
        AtomicInteger counter = new AtomicInteger();
        int task = 3 * 100;
        CountDownLatch latch = new CountDownLatch(task);
        for (int i = 0; i < task; i++) {
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    if (leakyBucket.tryAcquire()) {
                        counter.incrementAndGet();
                    }
                    latch.countDown();
                }
            });
        }
        latch.await(1000, TimeUnit.MILLISECONDS);
        Assert.assertEquals(leakyBucket.totalSize(), counter.get());
    }

    @Test
    public void testRelease() throws InterruptedException {
        int task = 3 * 100;
        CountDownLatch latch = new CountDownLatch(task);
        for (int i = 0; i < task; i++) {
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    if (leakyBucket.tryAcquire()) {
                        leakyBucket.release();
                    }
                    latch.countDown();
                }
            });
        }
        latch.await(10000, TimeUnit.MILLISECONDS);
        assertEquals(3, leakyBucket.references());
    }

    @Test
    public void testReset() {
        int tasks = 1;
        while(tasks++ < 20) {
            leakyBucket.tryAcquire();
            leakyBucket.release();
        }
        leakyBucket.reset();
        Assert.assertEquals(3, leakyBucket.references());
    }

    @Test
    public void testResize() throws InterruptedException {
        AtomicInteger counter = new AtomicInteger();
        int task = 3 * 100, newSize = 10;
        CountDownLatch latch = new CountDownLatch(task);
        CyclicBarrier barrier = new CyclicBarrier(task + 1);
        executors.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    barrier.await();
                } catch (Exception ignore) {
                }
                leakyBucket.resize(newSize);
            }
        });
        for (int i = 0; i < task; i++) {
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        barrier.await();
                    } catch (Exception ignore) {
                    }
                    if (leakyBucket.tryAcquire()) {
                        counter.incrementAndGet();
                    }
                    latch.countDown();
                }
            });
        }
        latch.await(1000, TimeUnit.MILLISECONDS);
        Assert.assertEquals(newSize, counter.get());
    }


    @Test
    public void testRefresh() throws InterruptedException {
        when(metaServerKeeperService.refreshKeeperContainerTokenStatus(any()))
                .thenReturn(new MetaServerKeeperService.KeeperContainerTokenStatusResponse(3, true));
        AtomicInteger counter = new AtomicInteger();
        int task = 3 * 100, newSize = 10;
        CountDownLatch latch = new CountDownLatch(task);
        CyclicBarrier barrier = new CyclicBarrier(task + 1);
        executors.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    barrier.await();
                } catch (Exception ignore) {
                }
                leakyBucket.refresh();
            }
        });
        for (int i = 0; i < task; i++) {
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        barrier.await();
                    } catch (Exception ignore) {
                    }
                    if (leakyBucket.tryAcquire()) {
                        counter.incrementAndGet();
                    }
                    latch.countDown();
                }
            });
        }

        latch.await(1000, TimeUnit.MILLISECONDS);
        Assert.assertTrue(3 < counter.get());
    }
}
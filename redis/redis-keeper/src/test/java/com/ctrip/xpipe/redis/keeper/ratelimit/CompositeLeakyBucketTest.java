package com.ctrip.xpipe.redis.keeper.ratelimit;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.container.KeeperContainerService;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
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

    @Spy
    private TestKeeperConfig keeperConfig = new TestKeeperConfig();

    @Before
    public void beforeCompositeLeakyBucketTest() {
        MockitoAnnotations.initMocks(this);
        when(keeperContainerService.list()).thenReturn(Lists.newArrayList());
        leakyBucket = new CompositeLeakyBucket(keeperConfig, metaServerKeeperService, keeperContainerService);
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
        Assert.assertEquals(leakyBucket.getTotalSize(), counter.get());
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
        leakyBucket.resize(0);
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
                        sleep(10);
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
        when(keeperConfig.isKeeperRateLimitOpen()).thenReturn(false);
        leakyBucket.setScheduled(scheduled);
        leakyBucket.checkKeeperConfigChange();
        sleep(110);
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

    @Test
    public void testCloseAndOpen() throws InterruptedException {
        //first close
        when(keeperConfig.isKeeperRateLimitOpen()).thenReturn(false);
        leakyBucket.setScheduled(scheduled);
        leakyBucket.checkKeeperConfigChange();
        sleep(110);
        AtomicInteger counter = new AtomicInteger();
        int task = 3 * 100, newSize = 10;
        CountDownLatch latch = new CountDownLatch(task);
        CyclicBarrier barrier = new CyclicBarrier(task + 1);
        CyclicBarrier finalBarrier2 = barrier;
        executors.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    finalBarrier2.await();
                } catch (Exception ignore) {
                }
                leakyBucket.refresh();
            }
        });
        for (int i = 0; i < task; i++) {
            CyclicBarrier finalBarrier3 = barrier;
            CountDownLatch finalLatch1 = latch;
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        finalBarrier3.await();
                    } catch (Exception ignore) {
                    }
                    if (leakyBucket.tryAcquire()) {
                        counter.incrementAndGet();
                    }
                    finalLatch1.countDown();
                }
            });
        }

        latch.await(1000, TimeUnit.MILLISECONDS);
        Assert.assertTrue(3 < counter.get());

        // second, open
        counter.set(0);
        when(keeperConfig.isKeeperRateLimitOpen()).thenReturn(true);
        leakyBucket.checkKeeperConfigChange();
        sleep(110);
        latch = new CountDownLatch(task);
        barrier = new CyclicBarrier(task + 1);
        CyclicBarrier finalBarrier = barrier;
        executors.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    finalBarrier.await();
                } catch (Exception ignore) {
                }
                leakyBucket.refresh();
            }
        });
        for (int i = 0; i < task; i++) {
            CyclicBarrier finalBarrier1 = barrier;
            CountDownLatch finalLatch = latch;
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        finalBarrier1.await();
                    } catch (Exception ignore) {
                    }
                    if (leakyBucket.tryAcquire()) {
                        counter.incrementAndGet();
                    }
                    finalLatch.countDown();
                }
            });
        }

        latch.await(1000, TimeUnit.MILLISECONDS);
        Assert.assertTrue(3 >= counter.get());
    }

    @Test
    public void testOpenAndCloseAndOpen() throws InterruptedException {
        //first open
        when(keeperConfig.isKeeperRateLimitOpen()).thenReturn(true);
        leakyBucket.setScheduled(scheduled);
        leakyBucket.checkKeeperConfigChange();
        sleep(200);
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

        latch.await(5000, TimeUnit.MILLISECONDS);
        Assert.assertEquals(3, counter.get());

        // second, close
        counter.set(0);
        when(keeperConfig.isKeeperRateLimitOpen()).thenReturn(false);
        sleep(200);
        CountDownLatch latch2 = new CountDownLatch(task);
        CyclicBarrier barrier2 = new CyclicBarrier(task + 1);
        executors.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    barrier2.await();
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
                        barrier2.await();
                    } catch (Exception ignore) {
                    }
                    if (leakyBucket.tryAcquire()) {
                        counter.incrementAndGet();
                    }
                    latch2.countDown();
                }
            });
        }

        latch2.await(5000, TimeUnit.MILLISECONDS);
        Assert.assertTrue(3 < counter.get());

        //third, open again
        counter.set(0);
        when(keeperConfig.isKeeperRateLimitOpen()).thenReturn(true);
        sleep(200);
        CountDownLatch latch3 = new CountDownLatch(task);
        CyclicBarrier barrier3 = new CyclicBarrier(task + 1);
        executors.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    barrier3.await();
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
                        barrier3.await();
                    } catch (Exception ignore) {
                    }
                    if (leakyBucket.tryAcquire()) {
                        counter.incrementAndGet();
                    }
                    latch3.countDown();
                }
            });
        }

        latch3.await(5000, TimeUnit.MILLISECONDS);
        Assert.assertEquals(3, counter.get());
    }

    @Test
    public void testReleaseAfterCloseLimit() {
        doReturn(true).when(keeperConfig).isKeeperRateLimitOpen();
        leakyBucket.setScheduled(scheduled);
        leakyBucket.checkKeeperConfigChange();
        sleep(200);

        int cnt = 0;
        while(leakyBucket.tryAcquire()) cnt++;

        doReturn(false).when(keeperConfig).isKeeperRateLimitOpen();
        sleep(200);

        IntStream.range(0, cnt).forEach(i -> leakyBucket.release());
        doReturn(true).when(keeperConfig).isKeeperRateLimitOpen();
        sleep(200);

        Assert.assertTrue(leakyBucket.tryAcquire());
    }
}
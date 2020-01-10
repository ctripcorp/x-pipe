package com.ctrip.xpipe.redis.console.migration;

import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class MultiThreadExecutorOptimizeTest extends AbstractService {

    private Logger logger = LoggerFactory.getLogger(MultiThreadExecutorOptimizeTest.class);

    private int maxThreads = 512;

    public ExecutorService getMigrationlExecutor() {
        //63337
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(maxThreads,
                maxThreads,
                120L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(maxThreads),
                XpipeThreadFactory.create("test-thread"),
                new ThreadPoolExecutor.CallerRunsPolicy());
        //109499
//        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(4,
//                maxThreads,
//                120L, TimeUnit.SECONDS,
//                new SynchronousQueue<>(),
//                XpipeThreadFactory.create("test-thread"),
//                new ThreadPoolExecutor.CallerRunsPolicy());
        threadPool.allowCoreThreadTimeOut(true);
        return MoreExecutors.getExitingExecutorService(
                threadPool,
                AbstractSpringConfigContext.THREAD_POOL_TIME_OUT, TimeUnit.SECONDS);
    }

    @Test
    public void testSleep30() throws InterruptedException {
        int tasks = 8000;
        CountDownLatch latch = new CountDownLatch(tasks);
        ExecutorService executors = getMigrationlExecutor();
        long start = System.nanoTime();
        for (int i = 0; i < tasks; i++) {
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    try {
//                        Thread.sleep(3200);
                        restTemplate.getForEntity("http://10.0.0.1:8080", Object.class);
                    } catch (Exception ignore) {

                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        latch.await(120, TimeUnit.SECONDS);
        logger.info("[duration] {}", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        Assert.assertEquals(0, latch.getCount());
    }

    @Test
    public void testRestTemplateTimeoutTime() {
        long start = System.nanoTime();
        try {
//            Thread.sleep(3000);
            restTemplate.getForEntity("http://10.0.0.1:8080", Object.class);
        } catch (Exception ignore) {

        }
        logger.info("[duration] {}", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
    }

}

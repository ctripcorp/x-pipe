package com.ctrip.xpipe.concurrent;

import com.ctrip.xpipe.AbstractTest;
import io.netty.util.internal.ConcurrentSet;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author wenchao.meng
 *         <p>
 *         May 10, 2017
 */
public class DefaultExecutorFactoryTest extends AbstractTest{


    @Test
    public void testThreadCount() throws InterruptedException, IOException, TimeoutException {

        int coreSize = 2;
        int keeperAliveTimeSeconds = 1;

        Set<Thread> threadSet = new ConcurrentSet<>();
        ExecutorService executorService = DefaultExecutorFactory.createAllowCoreTimeout(
                getTestName(), coreSize, keeperAliveTimeSeconds).createExecutorService();

        for(int i=0;i<coreSize;i++){

            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    logger.info("running");
                    threadSet.add(Thread.currentThread());
                }
            });
        }

        sleep(keeperAliveTimeSeconds * 2000);

        logger.info("size: {}", threadSet.size(), threadSet);
        Assert.assertEquals(coreSize, threadSet.size());
        for(Thread thread : threadSet){
            Assert.assertFalse(thread.isAlive());
        }
    }

    @Test
    public void testArgument(){

        int maxPoolSize = randomInt(100, 1000);
        int maxQueueSize = randomInt(100, 1000);
        int keeperAliveTime = randomInt(30, 60);

        RejectedExecutionHandler rejectedExecutionHandler = new ThreadPoolExecutor.AbortPolicy();
        DefaultExecutorFactory executorFactory = new DefaultExecutorFactory(getTestName(), 2, true, maxPoolSize, maxQueueSize, keeperAliveTime, TimeUnit.SECONDS, rejectedExecutionHandler);
        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executorFactory.createExecutorService();

        Assert.assertEquals(2, threadPoolExecutor.getCorePoolSize());
        Assert.assertEquals(maxPoolSize, threadPoolExecutor.getMaximumPoolSize());
        Assert.assertEquals(maxQueueSize, threadPoolExecutor.getQueue().remainingCapacity());
        Assert.assertEquals(keeperAliveTime, threadPoolExecutor.getKeepAliveTime(TimeUnit.SECONDS));
        Assert.assertEquals(rejectedExecutionHandler, threadPoolExecutor.getRejectedExecutionHandler());
    }

    @Test
    public void testCoreLessThanMax(){

        DefaultExecutorFactory executorFactory = new DefaultExecutorFactory(getTestName(), 2, 1);
        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executorFactory.createExecutorService();

        Assert.assertEquals(2, threadPoolExecutor.getCorePoolSize());
        Assert.assertEquals(2, threadPoolExecutor.getMaximumPoolSize());
    }
}

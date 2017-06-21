package com.ctrip.xpipe.concurrent;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

/**
 * @author wenchao.meng
 *         <p>
 *         May 10, 2017
 */
public class DefaultExecutorFactoryTest extends AbstractTest{


    @Test
    public void test() throws InterruptedException, IOException, TimeoutException {

        int coreSize = 2;
        int keeperAliveTimeSeconds = 1;

        List<Thread> threadList = new LinkedList<>();
        ExecutorService executorService = DefaultExecutorFactory.createAllowCoreTimeout(
                getTestName(), coreSize, keeperAliveTimeSeconds).createExecutorService();

        for(int i=0;i<coreSize;i++){

            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    threadList.add(Thread.currentThread());
                }
            });
        }

        sleep(keeperAliveTimeSeconds * 2000);

        Assert.assertEquals(coreSize, threadList.size());
        for(Thread thread : threadList){
            Assert.assertFalse(thread.isAlive());
        }
    }
}

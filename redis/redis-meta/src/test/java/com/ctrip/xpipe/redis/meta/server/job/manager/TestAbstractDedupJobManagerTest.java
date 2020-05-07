package com.ctrip.xpipe.redis.meta.server.job.manager;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.concurrent.OneThreadTaskExecutor;
import com.ctrip.xpipe.concurrent.TaskExecutor;
import com.ctrip.xpipe.utils.OsUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * May 07, 2020
 */
public class TestAbstractDedupJobManagerTest extends AbstractTest {

    private final static ExecutorService testThreadPool = Executors.newFixedThreadPool(OsUtils.getCpuCount());

    private final static OneThreadTaskExecutor oneTaskExecutor = new OneThreadTaskExecutor(testThreadPool);
    private AbstractDedupJobManager serial;

    //todo: parallel
//    private AbstractDedupJobManager parallel;

    private AtomicInteger counter = new AtomicInteger(0);

    @Before
    public void beforeTestAbstractDedupJobManagerTest() {
        serial = new TestSerialDedupJobManager();
    }

    @Test
    public void testOffer() throws TimeoutException {
        serial.offer(new CountingCommand(counter, 0));
        waitConditionUntilTimeOut(()->counter.get() >= 1, 100);
    }

    @Test
    public void testReplaceSuccess() throws Exception {
        // first command will execute as wish
        serial.offer(new CountingCommand(counter, 200));
        Thread.sleep(50);
        // second shall blocking as the first is sleeping
        serial.offer(new CountingCommand(counter, 10));
        // replace happened
        serial.offer(new CountingCommand(counter, 10));
        waitConditionUntilTimeOut(()->counter.get() >= 2, 500);
        //just ensure
        Thread.sleep(10);
        Assert.assertEquals(2, counter.get());
    }

    @Test
    public void testDedup() throws Exception {
        int tasks = 500;
        // first command will execute as wish
        CountingCommand command = new CountingCommand(counter, 50);
        command.addObserver(new CountingCommandListener() {
            @Override
            public void beforeSleep(CountingCommand command) {
            }

            @Override
            public void afterSleep(CountingCommand command) {
            }
        });

        serial.offer(command);
        for(int i = 0; i < tasks; i++) {
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    serial.offer(new CountingCommand(counter, 10));
                }
            });
        }


        waitConditionUntilTimeOut(()->counter.get() >= 2, 500);
        //just ensure
        waitConditionUntilTimeOut(()->oneTaskExecutor.isEmpty(), 1000);
        Assert.assertTrue(counter.get() < tasks && counter.get() > 1);
    }

    @Test
    public void testKeeperAddingWhileTaskRunning() throws TimeoutException, InterruptedException {
        // first command will execute as wish
        AtomicBoolean trigger = new AtomicBoolean(true);
        CountingCommand command = new CountingCommand(counter, 100);
        // second shall blocking as the first is sleeping
        command.addObserver(new CountingCommandListener() {
            @Override
            public void beforeSleep(CountingCommand command) {
            }

            @Override
            public void afterSleep(CountingCommand command) {
                trigger.set(false);
            }
        });
        serial.offer(command);

        while (trigger.get()) {
            serial.offer(new CountingCommand(counter, 100));
            Thread.sleep(10);
        }
        waitConditionUntilTimeOut(()->counter.get() >= 2, 500);

        //ensure
        Thread.sleep(50);
        Assert.assertEquals(2, counter.get());
    }



    private static class TestSerialDedupJobManager extends AbstractDedupJobManager {

        public TestSerialDedupJobManager() {
            super(oneTaskExecutor);
        }
    }
}
package com.ctrip.xpipe.concurrent;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 07, 2017
 */
public class FinalStateSetterManagerTest extends AbstractTest{

    private FinalStateSetterManager<String, String> finalStateSetterManager;

    private AtomicInteger count = new AtomicInteger();

    @Before
    public void beforeFinalStateSetterManagerTest(){

    }

    @Test
    public void testLazyTime() throws TimeoutException {
        String testKey = randomString(10);
        String testValue = randomString(10);

        finalStateSetterManager = new FinalStateSetterManager<>(executors,
                (String key) -> randomString(10),
                (key, value) -> count.incrementAndGet());
        finalStateSetterManager.LAZY_TIME_MILLI = 5;

        finalStateSetterManager.set(testKey, testValue);
        finalStateSetterManager.set(testKey, testValue);

        waitConditionUntilTimeOut(() -> count.get() == 1);

        sleep(10);
        finalStateSetterManager.set(testKey, testValue);
        waitConditionUntilTimeOut(() -> count.get() == 2);

        sleep(10);
        finalStateSetterManager.set(testKey, testValue);
        waitConditionUntilTimeOut(() -> count.get() == 3);
    }

    @Test
    public void testLazyTimeOnDifferentKeys() throws TimeoutException {
        String testKey = randomString(10);
        String anotherTestKey = randomString(10);
        String testValue = randomString(10);

        finalStateSetterManager = new FinalStateSetterManager<>(executors,
                (String key) -> randomString(10),
                (key, value) -> count.incrementAndGet());

        //make sure only 2 of 4 requests pass, despite time to start thread
        finalStateSetterManager.LAZY_TIME_MILLI = 60 * 1000;

        finalStateSetterManager.set(testKey, testValue);
        finalStateSetterManager.set(testKey, testValue);
        finalStateSetterManager.set(anotherTestKey, testValue);
        finalStateSetterManager.set(anotherTestKey, testValue);

        waitConditionUntilTimeOut(() -> count.get() == 2);

        //make sure lazy time expire
        finalStateSetterManager.LAZY_TIME_MILLI = 5;
        sleep(10);
        //not grow any more
        assertEquals(2, count.get());

        finalStateSetterManager.set(testKey, testValue);
        finalStateSetterManager.set(anotherTestKey, testValue);
        waitConditionUntilTimeOut(() -> count.get() == 4);
    }

    @Test
    public void testLazyTimeConcurrently() throws TimeoutException, InterruptedException {
        String testKey = randomString(10);
        String testValue = randomString(10);

        finalStateSetterManager = new FinalStateSetterManager<>(executors,
                (String key) -> randomString(10),
                (key, value) -> count.incrementAndGet());

        //make sure only 1 of 2 requests pass, despite time to start thread
        finalStateSetterManager.LAZY_TIME_MILLI = 60 * 1000;

        finalStateSetterManager.set(testKey, testValue);
        finalStateSetterManager.set(testKey, testValue);

        waitConditionUntilTimeOut(() -> count.get() == 1);

        //make sure lazy time expire
        finalStateSetterManager.LAZY_TIME_MILLI = 5;
        sleep(10);

        int concurrency = 100;
        ExecutorService executors = Executors.newFixedThreadPool(concurrency);
        CountDownLatch start = new CountDownLatch(concurrency);
        CountDownLatch end = new CountDownLatch(concurrency);
        for (int i = 0; i < concurrency; i++) {
            executors.submit(() -> {
                start.countDown();
                try {
                    start.await();
                } catch (InterruptedException ignore) {
                }
                finalStateSetterManager.set(testKey, testValue);
                end.countDown();
            });
        }
        end.await();
        waitConditionUntilTimeOut(() -> count.get() == 2);

        sleep(10);
        //not grow any more
        assertEquals(2, count.get());
    }


    @Test
    public void testSet() throws TimeoutException {

        String testKey = randomString(10);

        finalStateSetterManager = new FinalStateSetterManager<>(executors,
                (String key) -> randomString(10),
                (key, value) -> count.incrementAndGet());

        finalStateSetterManager.set(testKey, randomString(10));

        sleep(100);

        finalStateSetterManager.set(testKey, randomString(10));

        waitConditionUntilTimeOut(() -> count.get() == 2);
    }

    @Test
    public void testMultiExecuteFinal() throws TimeoutException {

        String testKey = randomString(10);

        finalStateSetterManager = new FinalStateSetterManager<>(executors,
                (String key) -> randomString(10),
                (key, value) -> {

                    int current = count.incrementAndGet();
                    if( current == 1){
                        sleep(500);
                    }
                });

        finalStateSetterManager.set(testKey, randomString(10));

        sleep(100);

        for(int i=0;i<10;i++){
            finalStateSetterManager.set(testKey, randomString(10));
        }

        sleep(1000);

        waitConditionUntilTimeOut(() -> count.get() == 2);
    }


}

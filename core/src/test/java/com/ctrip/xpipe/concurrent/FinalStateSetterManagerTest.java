package com.ctrip.xpipe.concurrent;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

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

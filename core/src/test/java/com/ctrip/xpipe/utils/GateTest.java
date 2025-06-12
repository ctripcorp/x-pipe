package com.ctrip.xpipe.utils;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 29, 2018
 */
public class GateTest extends AbstractTest {

    private Gate gate;

    @Before
    public void beforeGateTest() {
        gate = new Gate(getTestName());
    }


    @Test
    public void testMultiOpenClose() throws TimeoutException {

        int round = 10;
        for (int i = 0; i < round; i++) {

            gate.close();
            final AtomicBoolean passed = new AtomicBoolean(false);
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    gate.tryPass();
                    passed.set(true);
                }
            });

            waitConditionUntilTimeOut(()->!passed.get());
            gate.open();
            waitConditionUntilTimeOut(passed::get);
        }
    }

    @Test
    public void testOpenClose() throws InterruptedException, TimeoutException {

        int passengers = 10;
        int closeTimeMilli = 100;
        final AtomicInteger passThroughPassengers = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(passengers);

        for (int i = 0; i < passengers; i++) {
            executors.execute(new Runnable() {
                @Override
                public void run() {

                    latch.countDown();
                    for (int i = 0; i < 10000; i++) {

                        long begin = System.currentTimeMillis();
                        gate.tryPass();
                        long end = System.currentTimeMillis();

                        int duration = (int) (end - begin);

                        if (duration < 3) {
                            sleep(1);
                        }
                        if (duration > closeTimeMilli / 2) {
                            logger.info("[through gate ]");
                            passThroughPassengers.incrementAndGet();
                            break;
                        }
                    }
                }
            });
        }

        latch.await(1, TimeUnit.SECONDS);

        logger.info("[close gate]");
        gate.close();
        sleep(closeTimeMilli);
        logger.info("[open gate]");
        gate.open();

        waitConditionUntilTimeOut(() -> passengers == passThroughPassengers.get());
    }
}

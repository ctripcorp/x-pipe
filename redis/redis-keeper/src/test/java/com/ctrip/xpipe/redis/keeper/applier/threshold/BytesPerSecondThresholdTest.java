package com.ctrip.xpipe.redis.keeper.applier.threshold;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertTrue;

public class BytesPerSecondThresholdTest extends AbstractTest {

    long seconds = 2;

    long bytes = 10;

    AtomicLong result = new AtomicLong(0);

    @Test
    public void test() {

        BytesPerSecondThreshold qpsThreshold = new BytesPerSecondThreshold(1000, scheduled);

        long start = System.currentTimeMillis();
        while(System.currentTimeMillis() - start < seconds * 1000) {
            qpsThreshold.tryPass(bytes);
            executors.submit(() -> {
                result.addAndGet(bytes);
            });
        }

        assertTrue(result.get() <= ((seconds+1) * 1000));

        logger.info("result: " + result.get());
    }
}
package com.ctrip.xpipe.redis.keeper.applier.threshold;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

/**
 * @author Slight
 * <p>
 * Dec 14, 2022 11:58
 */
public class QPSThresholdTest extends AbstractTest {

    long seconds = 2;

    long qps = 1000;

    AtomicLong result = new AtomicLong(0);

    @Test
    public void test() {

        QPSThreshold qpsThreshold = new QPSThreshold(1000, scheduled);

        long start = System.currentTimeMillis();
        while(System.currentTimeMillis() - start < seconds * 1000) {
            qpsThreshold.tryPass();
            executors.submit(() -> {
                result.addAndGet(1);
            });
        }

        assertTrue(result.get() <= (seconds + 1 /*not that accurate*/ ) * qps);

        logger.info("result: " + result.get());
    }
}
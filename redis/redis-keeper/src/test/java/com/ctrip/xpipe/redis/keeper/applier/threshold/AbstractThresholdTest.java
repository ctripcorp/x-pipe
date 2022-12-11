package com.ctrip.xpipe.redis.keeper.applier.threshold;

import com.ctrip.xpipe.AbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * @author Slight
 * <p>
 * Dec 09, 2022 12:49
 */
public class AbstractThresholdTest extends AbstractTest {

    private ExecutorService executor;

    @Before
    public void setUp() throws Exception {
        executor = Executors.newFixedThreadPool(8);
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdownNow();
    }

    private static long limit = 10000;

    private static int maxPacket = 2000;

    private static long duration = 2000;

    public static class TestThreshold extends AbstractThreshold {

        private ScheduledExecutorService scheduled = Executors.newSingleThreadScheduledExecutor();

        private ScheduledFuture<?> toBeClosed;

        public TestThreshold(long limit) {
            super(limit);
            becomeVisible();
        }

        public void becomeVisible() {
            toBeClosed = scheduled.scheduleAtFixedRate(()->{
                long acc = accumulated.get();
                logger.info("[accumulated] {}", acc);
                assertTrue(acc <= limit + maxPacket);
            }, duration/30, duration/30, TimeUnit.MILLISECONDS);
        }

        public void close() {
            toBeClosed.cancel(true);
        }
    }

    @Test
    public void testStability() {

        long start = System.currentTimeMillis();

        TestThreshold threshold = new TestThreshold(limit);

        try {
            while (System.currentTimeMillis() - start <= duration) {
                //this is like a netty thread
                long quantity = randomInt(1, maxPacket);
                threshold.tryPass(quantity);
                executor.submit(() -> {
                    sleep(100);
                    threshold.release(quantity);
                });
            }
        } finally {
            threshold.close();
        }
    }
}
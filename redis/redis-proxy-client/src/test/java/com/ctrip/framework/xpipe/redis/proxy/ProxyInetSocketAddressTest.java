package com.ctrip.framework.xpipe.redis.proxy;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Slight
 * <p>
 * Sep 11, 2021 10:02 AM
 */
public class ProxyInetSocketAddressTest {

    @Test
    public void testConcurrentSafe() throws InterruptedException {
        int count = 10;
        Executor executor = Executors.newFixedThreadPool(count);
        int times = 100000;
        CountDownLatch begin = new CountDownLatch(count);
        CountDownLatch end = new CountDownLatch(count);
        ProxyInetSocketAddress proxy = new ProxyInetSocketAddress(3306);
        for (int i = 0; i < count; i++) {
            executor.execute(() -> {
                begin.countDown();
                try {
                    begin.await();
                } catch (InterruptedException interrupted) {
                }
                for (int j = 0; j < times; j++) {
                    proxy.tryDown(count * times);
                }
                end.countDown();
            });
        }
        end.await();
        assertEquals(times * count, proxy.retryDownCounter);
        assertTrue(proxy.down);
    }
}
package com.ctrip.xpipe.redis.console;

import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public class JedisTest {

    @Test
    public void testJedis() throws InterruptedException {
        Jedis jedis1 = new Jedis("10.5.110.232", 12000);
        Jedis jedis2 = new Jedis("10.5.110.250", 12000);

        jedis1.connect();
        jedis2.connect();

        CyclicBarrier barrier = new CyclicBarrier(2);
        CountDownLatch latch = new CountDownLatch(2);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    barrier.await();
//                    jedis1.expire("key-hash", 20);
                    jedis2.expire("key-hash-1", 120);
                    latch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                } finally {
                    jedis2.close();
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    barrier.await();
                    jedis1.hset("key-hash-1", "field2", "val2");
//                    jedis2.set("key-2", "val2");
//                    jedis2.expire("key-2", 120);
                    latch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                } finally {
                    jedis1.close();
                }
            }
        }).start();

        latch.await();
    }
}

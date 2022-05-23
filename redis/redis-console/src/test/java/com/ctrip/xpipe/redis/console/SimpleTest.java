package com.ctrip.xpipe.redis.console;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.gtid.GtidSet;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

/**
 * @author wenchao.meng
 * <p>
 * Sep 29, 2019
 */
public class SimpleTest extends AbstractTest {


    private String key = randomString(10);
    private String value = randomString(1 << 20);

    @Test
    public void testConcurrentWrite() throws InterruptedException {

        Jedis jedis1 = new Jedis("127.0.0.1", 6379);
        Jedis jedis2 = new Jedis("127.0.0.1", 6479);


        Jedis[] jedises = new Jedis[]{
                jedis1, jedis2
        };

        int masterCount = jedises.length;
        int round = 0;

        logger.info("key:{}", key);

        while (true) {

            round++;

            Set<String> values = write(jedises, round);

            assertEquals(jedises, values, round);

            delete(jedises, round);
            sleep(500);
        }
    }

    @Test
    public void testGtidSetContain() {
        GtidSet gtidSet1 = new GtidSet("a1:1-100");
        GtidSet gtidSet2 = new GtidSet("a1:1-105");
        logger.info("[contain] {}", gtidSet1.isContainedWithin(gtidSet2));
        logger.info("[contain] {}", gtidSet2.isContainedWithin(gtidSet1));
        logger.info("[subtract] {}", gtidSet2.subtract(gtidSet1));
        logger.info("[subtract] {}", gtidSet1.subtract(gtidSet2));
    }

    private void assertEquals(Jedis[] jedises, Set<String> values, int round) {

        logger.info("assertEquals");

        String key = getkey(round);

        for (Jedis jedis : jedises) {
            String value = jedis.get(key);
            Assert.assertTrue(values.contains(value));
        }

    }

    private void delete(Jedis[] jedises, int round) {

        logger.info("delete");

        for (Jedis jedis : jedises) {
            jedis.del(getkey(round));
        }
    }

    private Set<String> write(Jedis[] jedises, int round) throws InterruptedException {

        logger.info("write");

        final CyclicBarrier barrier = new CyclicBarrier(jedises.length);
        final CountDownLatch latch = new CountDownLatch(jedises.length);

        String key = getkey(round);
        Set<String> values = new HashSet<>();

        for (Jedis jedis : jedises) {
            executors.execute(new Runnable() {
                @Override
                public void run() {

                    try {

                        String value = getValue(round, jedis);
                        logger.info("begin wait");

                        barrier.await();
                        logger.info("set to {} key:{}", jedis, key);
                        jedis.set(key, value);
                        values.add(value);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (BrokenBarrierException e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        latch.await();

        return values;
    }

    private String getValue(int round, Jedis jedis) {

        return String.format("%s-%d-%s", value, round, jedis);
    }

    private String getkey(int round) {

        return key + "-" + round;
    }

}

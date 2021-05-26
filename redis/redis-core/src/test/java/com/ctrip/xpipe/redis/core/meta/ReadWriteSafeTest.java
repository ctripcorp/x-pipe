package com.ctrip.xpipe.redis.core.meta;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Slight
 * <p>
 * May 13, 2021 5:50 PM
 */
public class ReadWriteSafeTest {

    private long waitTime = 300;

    @Test
    public void writeBlockWrite() throws InterruptedException {
        ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        ReadWriteSafe safe = () -> readWriteLock;
        CountDownLatch latch = new CountDownLatch(1);
        readWriteLock.writeLock().lock();
        new Thread(()-> { safe.write(latch::countDown); }).start();
        assertFalse(latch.await(waitTime, TimeUnit.MILLISECONDS));
    }

    @Test
    public void writeBlockRead() throws InterruptedException {
        ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        ReadWriteSafe safe = () -> readWriteLock;
        CountDownLatch latch = new CountDownLatch(1);
        readWriteLock.writeLock().lock();
        new Thread(()-> { safe.read(()->{ latch.countDown(); return 1; }); }).start();
        assertFalse(latch.await(waitTime, TimeUnit.MILLISECONDS));
    }

    @Test
    public void readBlockWrite() throws InterruptedException {
        ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        ReadWriteSafe safe = () -> readWriteLock;
        CountDownLatch latch = new CountDownLatch(1);
        readWriteLock.readLock().lock();
        new Thread(()-> { safe.write(latch::countDown); }).start();
        assertFalse(latch.await(waitTime, TimeUnit.MILLISECONDS));
    }

    @Test
    public void readDontBlockRead() throws InterruptedException {
        ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        ReadWriteSafe safe = () -> readWriteLock;
        CountDownLatch latch = new CountDownLatch(1);
        readWriteLock.readLock().lock();
        new Thread(()-> { safe.read(()->{ latch.countDown(); return 1; }); }).start();
        assertTrue(latch.await(waitTime, TimeUnit.MILLISECONDS));
    }

}
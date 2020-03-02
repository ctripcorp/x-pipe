package com.ctrip.xpipe.redis.keeper.ratelimit;

import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;
import java.util.function.IntSupplier;

/**
 * @author chen.zhu
 * <p>
 * Feb 17, 2020
 */
public class DefaultLeakyBucket implements LeakyBucket {

    private static final Logger logger = LoggerFactory.getLogger(DefaultLeakyBucket.class);

    public static final String DEFAULT_BUCKET_SIZE = "3";

    private volatile Semaphore semaphore;

    private volatile int totalSize;

    public DefaultLeakyBucket(int initSize) {
        this.semaphore = new Semaphore(initSize);
        this.totalSize = initSize;
    }

    public synchronized void release() {
        logger.info("[release][before] {}", references());
        semaphore.release();
        logger.info("[release][after] {}", references());
    }

    public synchronized boolean tryAcquire() {
        logger.info("[tryAcquire][before] {}", references());
        boolean result = semaphore.tryAcquire();
        logger.info("[tryAcquire][after] {}", references());
        return result;
    }

    public void resize(int newSize) {
        if (newSize < 1) {
            reset();
            return;
        }
        synchronized (this) {
            int currentSize = getTotalSize();
            int borrowed = currentSize - semaphore.availablePermits();
            semaphore = new Semaphore(newSize);
            semaphore.tryAcquire(borrowed);
            totalSize = newSize;
            logger.warn("[resize] from {} to {}", currentSize, newSize);
        }
    }

    public void reset() {
        synchronized (this) {
            semaphore = new Semaphore(getTotalSize());
        }
    }

    @VisibleForTesting
    public synchronized int references() {
        return semaphore.availablePermits();
    }

    @Override
    public int getTotalSize() {
        return totalSize;
    }
}

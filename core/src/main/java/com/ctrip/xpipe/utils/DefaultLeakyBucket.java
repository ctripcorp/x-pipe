package com.ctrip.xpipe.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;

/**
 * @author chen.zhu
 * <p>
 * Feb 17, 2020
 */
public class DefaultLeakyBucket implements LeakyBucket {

    private static final Logger logger = LoggerFactory.getLogger(DefaultLeakyBucket.class);
    private static final int INCREMENTAL_NUM = 1;

    private IntSupplier tokens;
    private final AtomicInteger size;

    public DefaultLeakyBucket(IntSupplier tokens) {
        this.tokens = tokens;
        size = new AtomicInteger(tokens.getAsInt());
    }

    public void release() {
        logger.info("[release][before] {}", references());
        returnToken();
        logger.info("[release][after] {}", references());
    }

    public boolean tryAcquire() {
        logger.info("[tryAcquire][before] {}", references());
        if (size.get() > 0) {
            synchronized (this) {
                if (size.get() > 0) {
                    size.decrementAndGet();
                    logger.info("[tryAcquire][after] {}", references());
                    return true;
                }
            }
        }
        return false;
    }

    public void resize(int newSize) {
        synchronized (this) {
            int currentSize = totalSize();
            tokens = new IntSupplier() {
                @Override
                public int getAsInt() {
                    return newSize;
                }
            };
            int delta = newSize - currentSize;
            size.getAndAdd(delta);
            logger.warn("[resize] from {} to {}", currentSize, newSize);
        }
    }

    public void reset() {
        synchronized (this) {
            size.set(totalSize());
        }
    }

    private void returnToken() {
        if (size.get() < totalSize()) {
            synchronized (this) {
                if (size.get() < totalSize()) {
                    size.set(Math.min(size.get() + INCREMENTAL_NUM, tokens.getAsInt()));
                }
            }
        }
    }

    @VisibleForTesting
    public int references() {
        return size.get();
    }

    @Override
    public int totalSize() {
        return tokens.getAsInt();
    }
}

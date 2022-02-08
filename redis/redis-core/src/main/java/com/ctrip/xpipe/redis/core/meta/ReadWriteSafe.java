package com.ctrip.xpipe.redis.core.meta;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Supplier;

/**
 * @author Slight
 * <p>
 * May 13, 2021 4:52 PM
 */
public interface ReadWriteSafe {

    ReadWriteLock getReadWriteLock();

    default void read(Runnable runnable) {
        getReadWriteLock().readLock().lock();
        try {
            runnable.run();
        } finally {
            getReadWriteLock().readLock().unlock();
        }
    }

    default <T> T read(Supplier<T> supplier) {
        getReadWriteLock().readLock().lock();
        try {
            return supplier.get();
        } finally {
            getReadWriteLock().readLock().unlock();
        }
    }

    default void write(Runnable runnable) {
        getReadWriteLock().writeLock().lock();
        try {
            runnable.run();
        } finally {
            getReadWriteLock().writeLock().unlock();
        }
    }

    default <T> T write(Supplier<T> supplier) {
        getReadWriteLock().writeLock().lock();
        try {
            return supplier.get();
        } finally {
            getReadWriteLock().writeLock().unlock();
        }
    }
}

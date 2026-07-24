package com.ctrip.xpipe.redis.keeper.storage;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

final class CacheMemoryTracker {

    private final AtomicLong committedBytes = new AtomicLong(0);
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition available = lock.newCondition();

    long committedBytes() {
        return committedBytes.get();
    }

    private boolean reserve(long bytes, long limitBytes) {
        while (true) {
            long current = committedBytes.get();
            long next = current + bytes;
            if (next > limitBytes) return false;
            if (committedBytes.compareAndSet(current, next)) return true;
        }
    }

    boolean tryReserve(long bytes, long limitBytes) {
        if (bytes == 0) return true;
        return reserve(bytes, limitBytes);
    }

    void reserve(long bytes, long limitBytes, long timeoutMs) {
        // fast path: lock-free CAS
        if (tryReserve(bytes, limitBytes)) return;
        // slow path: wait with timeout
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        lock.lock();
        try {
            while (true) {
                if (tryReserve(bytes, limitBytes)) return;
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new CacheMemoryReserveException(bytes, limitBytes, committedBytes.get());
                }
                available.awaitNanos(remaining);
            }
        } catch (CacheMemoryReserveException e) {
            throw e;
        } catch (Exception e) {
            CacheMemoryReserveException ex = new CacheMemoryReserveException(bytes, limitBytes, committedBytes.get());
            ex.addSuppressed(e);
            throw ex;
        } finally {
            lock.unlock();
        }
    }

    void release(long bytes) {
        if (bytes == 0) return;
        committedBytes.addAndGet(-bytes);
        // signal loss may happen. but it's ok since other release operations will also signal or wait at most timeoutMs.
        if (lock.hasWaiters(available)) {
            lock.lock();
            try {
                available.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }
}

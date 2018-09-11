package com.ctrip.xpipe.pool.object;

/**
 * @author chen.zhu
 * <p>
 * Sep 11, 2018
 */
import java.util.Collection;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This sub-class was created to expose the waiting threads so that they can be
 * interrupted when the pool using the queue that uses this lock is closed. The
 * class is intended for internal use only.
 * <p>
 * This class is intended to be thread-safe.
 *
 * @since 2.0
 */
class InterruptibleReentrantLock extends ReentrantLock {

    private static final long serialVersionUID = 1L;

    /**
     * Create a new InterruptibleReentrantLock with the given fairness policy.
     *
     * @param fairness true means threads should acquire contended locks as if
     * waiting in a FIFO queue
     */
    public InterruptibleReentrantLock(boolean fairness) {
        super(fairness);
    }

    /**
     * Interrupt the threads that are waiting on a specific condition
     *
     * @param condition the condition on which the threads are waiting.
     */
    public void interruptWaiters(Condition condition) {
        Collection<Thread> threads = getWaitingThreads(condition);
        for (Thread thread : threads) {
            thread.interrupt();
        }
    }
}
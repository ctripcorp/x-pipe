package com.ctrip.xpipe.utils;

import static org.junit.Assert.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import com.ctrip.xpipe.JSR166TestCase;
import junit.framework.AssertionFailedError;
import junit.framework.TestSuite;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * Jul 22, 2020
 */
public class AbstractQueuedSynchronizerTest extends JSR166TestCase {


    /**
     * A simple mutex class, adapted from the class javadoc.  Exclusive
     * acquire tests exercise this as a sample user extension.
     *
     * Unlike the javadoc sample, we don't track owner thread via
     * AbstractOwnableSynchronizer methods.
     */
    static class Mutex extends AbstractQueuedSynchronizer {
        /** An eccentric value > 32 bits for locked synchronizer state. */
        static final long LOCKED = (1L << 63) | (1L << 15);

        static final long UNLOCKED = 0;

        /** Owner thread is untracked, so this is really just isLocked(). */
        @Override public boolean isHeldExclusively() {
            long state = getState();
            assertTrue(state == UNLOCKED || state == LOCKED);
            return state == LOCKED;
        }

        @Override protected boolean tryAcquire(long acquires) {
            assertEquals(LOCKED, acquires);
            return compareAndSetState(UNLOCKED, LOCKED);
        }

        protected boolean tryRelease(long releases) {
            if (getState() != LOCKED) throw new IllegalMonitorStateException();
            setState(UNLOCKED);
            return true;
        }

        public boolean tryAcquireNanos(long nanos) throws InterruptedException {
            return tryAcquireNanos(LOCKED, nanos);
        }

        public boolean tryAcquire() {
            return tryAcquire(LOCKED);
        }

        public boolean tryRelease() {
            return tryRelease(LOCKED);
        }

        public void acquire() {
            acquire(LOCKED);
        }

        public void acquireInterruptibly() throws InterruptedException {
            acquireInterruptibly(LOCKED);
        }

        public void release() {
            release(LOCKED);
        }

        /** Faux-Implements Lock.newCondition(). */
        public AbstractQueuedSynchronizer.ConditionObject newCondition() {
            return new AbstractQueuedSynchronizer.ConditionObject();
        }
    }

    /**
     * A minimal latch class, to test shared mode.
     */
    static class BooleanLatch extends AbstractQueuedSynchronizer {
        public boolean isSignalled() { return getState() != 0; }

        public int tryAcquireShared(long ignore) {
            return isSignalled() ? 1 : -1;
        }

        public boolean tryReleaseShared(long ignore) {
            setState(1L << 62);
            return true;
        }
    }

    /**
     * A runnable calling acquireInterruptibly that does not expect to
     * be interrupted.
     */
    class InterruptibleSyncRunnable extends CheckedRunnable {
        final Mutex sync;
        InterruptibleSyncRunnable(Mutex sync) { this.sync = sync; }
        public void realRun() throws InterruptedException {
            sync.acquireInterruptibly();
        }
    }

    /**
     * A runnable calling acquireInterruptibly that expects to be
     * interrupted.
     */
    class InterruptedSyncRunnable extends CheckedInterruptedRunnable {
        final Mutex sync;
        InterruptedSyncRunnable(Mutex sync) { this.sync = sync; }
        public void realRun() throws InterruptedException {
            sync.acquireInterruptibly();
        }
    }

    /** A constant to clarify calls to checking methods below. */
    static final Thread[] NO_THREADS = new Thread[0];

    /**
     * Spin-waits until sync.isQueued(t) becomes true.
     */
    void waitForQueuedThread(AbstractQueuedSynchronizer sync,
                             Thread t) {
        long startTime = System.nanoTime();
        while (!sync.isQueued(t)) {
            if (millisElapsedSince(startTime) > LONG_DELAY_MS)
                throw new AssertionFailedError("timed out");
            Thread.yield();
        }
        assertTrue(t.isAlive());
    }

    /**
     * Checks that sync has exactly the given queued threads.
     */
    void assertHasQueuedThreads(AbstractQueuedSynchronizer sync,
                                Thread... expected) {
        Collection<Thread> actual = sync.getQueuedThreads();
        assertEquals(expected.length > 0, sync.hasQueuedThreads());
        assertEquals(expected.length, sync.getQueueLength());
        assertEquals(expected.length, actual.size());
        assertEquals(expected.length == 0, actual.isEmpty());
        assertEquals(new HashSet<Thread>(actual),
                new HashSet<Thread>(Arrays.asList(expected)));
    }

    /**
     * Checks that sync has exactly the given (exclusive) queued threads.
     */
    void assertHasExclusiveQueuedThreads(AbstractQueuedSynchronizer sync,
                                         Thread... expected) {
        assertHasQueuedThreads(sync, expected);
        assertEquals(new HashSet<Thread>(sync.getExclusiveQueuedThreads()),
                new HashSet<Thread>(sync.getQueuedThreads()));
        assertEquals(0, sync.getSharedQueuedThreads().size());
        assertTrue(sync.getSharedQueuedThreads().isEmpty());
    }

    /**
     * Checks that sync has exactly the given (shared) queued threads.
     */
    void assertHasSharedQueuedThreads(AbstractQueuedSynchronizer sync,
                                      Thread... expected) {
        assertHasQueuedThreads(sync, expected);
        assertEquals(new HashSet<Thread>(sync.getSharedQueuedThreads()),
                new HashSet<Thread>(sync.getQueuedThreads()));
        assertEquals(0, sync.getExclusiveQueuedThreads().size());
        assertTrue(sync.getExclusiveQueuedThreads().isEmpty());
    }

    /**
     * Checks that condition c has exactly the given waiter threads,
     * after acquiring mutex.
     */
    void assertHasWaitersUnlocked(Mutex sync, AbstractQueuedSynchronizer.ConditionObject c,
                                  Thread... threads) {
        sync.acquire();
        assertHasWaitersLocked(sync, c, threads);
        sync.release();
    }

    /**
     * Checks that condition c has exactly the given waiter threads.
     */
    void assertHasWaitersLocked(Mutex sync, AbstractQueuedSynchronizer.ConditionObject c,
                                Thread... threads) {
        assertEquals(threads.length > 0, sync.hasWaiters(c));
        assertEquals(threads.length, sync.getWaitQueueLength(c));
        assertEquals(threads.length == 0, sync.getWaitingThreads(c).isEmpty());
        assertEquals(threads.length, sync.getWaitingThreads(c).size());
        assertEquals(new HashSet<Thread>(sync.getWaitingThreads(c)),
                new HashSet<Thread>(Arrays.asList(threads)));
    }

    enum AwaitMethod { await, awaitTimed, awaitNanos, awaitUntil }

    /**
     * Awaits condition using the specified AwaitMethod.
     */
    void await(AbstractQueuedSynchronizer.ConditionObject c, AwaitMethod awaitMethod)
            throws InterruptedException {
        long timeoutMillis = 2 * LONG_DELAY_MS;
        switch (awaitMethod) {
            case await:
                c.await();
                break;
            case awaitTimed:
                assertTrue(c.await(timeoutMillis, MILLISECONDS));
                break;
            case awaitNanos:
                long nanosTimeout = MILLISECONDS.toNanos(timeoutMillis);
                long nanosRemaining = c.awaitNanos(nanosTimeout);
                assertTrue(nanosRemaining > 0);
                break;
            case awaitUntil:
                assertTrue(c.awaitUntil(delayedDate(timeoutMillis)));
                break;
            default:
                throw new AssertionError();
        }
    }

    /**
     * Checks that awaiting the given condition times out (using the
     * default timeout duration).
     */
    void assertAwaitTimesOut(AbstractQueuedSynchronizer.ConditionObject c, AwaitMethod awaitMethod) {
        final long timeoutMillis = timeoutMillis();
        final long startTime;
        try {
            switch (awaitMethod) {
                case awaitTimed:
                    startTime = System.nanoTime();
                    assertFalse(c.await(timeoutMillis, MILLISECONDS));
                    assertTrue(millisElapsedSince(startTime) >= timeoutMillis);
                    break;
                case awaitNanos:
                    startTime = System.nanoTime();
                    long nanosTimeout = MILLISECONDS.toNanos(timeoutMillis);
                    long nanosRemaining = c.awaitNanos(nanosTimeout);
                    assertTrue(nanosRemaining <= 0);
                    assertTrue(nanosRemaining > -MILLISECONDS.toNanos(LONG_DELAY_MS));
                    assertTrue(millisElapsedSince(startTime) >= timeoutMillis);
                    break;
                case awaitUntil:
                    // We shouldn't assume that nanoTime and currentTimeMillis
                    // use the same time source, so don't use nanoTime here.
                    java.util.Date delayedDate = delayedDate(timeoutMillis);
                    assertFalse(c.awaitUntil(delayedDate(timeoutMillis)));
                    assertTrue(new java.util.Date().getTime() >= delayedDate.getTime());
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        } catch (InterruptedException ie) { threadUnexpectedException(ie); }
    }

    /**
     * isHeldExclusively is false upon construction
     */
    @Test
    public void testIsHeldExclusively() {
        Mutex sync = new Mutex();
        assertFalse(sync.isHeldExclusively());
    }

    /**
     * acquiring released sync succeeds
     */
    @Test
    public void testAcquire() {
        Mutex sync = new Mutex();
        sync.acquire();
        assertTrue(sync.isHeldExclusively());
        sync.release();
        assertFalse(sync.isHeldExclusively());
    }

    /**
     * tryAcquire on a released sync succeeds
     */
    @Test
    public void testTryAcquire() {
        Mutex sync = new Mutex();
        assertTrue(sync.tryAcquire());
        assertTrue(sync.isHeldExclusively());
        sync.release();
        assertFalse(sync.isHeldExclusively());
    }

    /**
     * hasQueuedThreads reports whether there are waiting threads
     */
    @Test
    public void testHasQueuedThreads() {
        final Mutex sync = new Mutex();
        assertFalse(sync.hasQueuedThreads());
        sync.acquire();
        Thread t1 = newStartedThread(new InterruptedSyncRunnable(sync));
        waitForQueuedThread(sync, t1);
        assertTrue(sync.hasQueuedThreads());
        Thread t2 = newStartedThread(new InterruptibleSyncRunnable(sync));
        waitForQueuedThread(sync, t2);
        assertTrue(sync.hasQueuedThreads());
        t1.interrupt();
        awaitTermination(t1);
        assertTrue(sync.hasQueuedThreads());
        sync.release();
        awaitTermination(t2);
        assertFalse(sync.hasQueuedThreads());
    }

    /**
     * isQueued(null) throws NullPointerException
     */
    @Test
    public void testIsQueuedNPE() {
        final Mutex sync = new Mutex();
        try {
            sync.isQueued(null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * isQueued reports whether a thread is queued
     */
    @Test
    public void testIsQueued() {
        final Mutex sync = new Mutex();
        Thread t1 = new Thread(new InterruptedSyncRunnable(sync));
        Thread t2 = new Thread(new InterruptibleSyncRunnable(sync));
        assertFalse(sync.isQueued(t1));
        assertFalse(sync.isQueued(t2));
        sync.acquire();
        t1.start();
        waitForQueuedThread(sync, t1);
        assertTrue(sync.isQueued(t1));
        assertFalse(sync.isQueued(t2));
        t2.start();
        waitForQueuedThread(sync, t2);
        assertTrue(sync.isQueued(t1));
        assertTrue(sync.isQueued(t2));
        t1.interrupt();
        awaitTermination(t1);
        assertFalse(sync.isQueued(t1));
        assertTrue(sync.isQueued(t2));
        sync.release();
        awaitTermination(t2);
        assertFalse(sync.isQueued(t1));
        assertFalse(sync.isQueued(t2));
    }

    /**
     * getFirstQueuedThread returns first waiting thread or null if none
     */
    @Test
    public void testGetFirstQueuedThread() {
        final Mutex sync = new Mutex();
        assertNull(sync.getFirstQueuedThread());
        sync.acquire();
        Thread t1 = newStartedThread(new InterruptedSyncRunnable(sync));
        waitForQueuedThread(sync, t1);
        assertEquals(t1, sync.getFirstQueuedThread());
        Thread t2 = newStartedThread(new InterruptibleSyncRunnable(sync));
        waitForQueuedThread(sync, t2);
        assertEquals(t1, sync.getFirstQueuedThread());
        t1.interrupt();
        awaitTermination(t1);
        assertEquals(t2, sync.getFirstQueuedThread());
        sync.release();
        awaitTermination(t2);
        assertNull(sync.getFirstQueuedThread());
    }

    /**
     * hasContended reports false if no thread has ever blocked, else true
     */
    @Test
    public void testHasContended() {
        final Mutex sync = new Mutex();
        assertFalse(sync.hasContended());
        sync.acquire();
        assertFalse(sync.hasContended());
        Thread t1 = newStartedThread(new InterruptedSyncRunnable(sync));
        waitForQueuedThread(sync, t1);
        assertTrue(sync.hasContended());
        Thread t2 = newStartedThread(new InterruptibleSyncRunnable(sync));
        waitForQueuedThread(sync, t2);
        assertTrue(sync.hasContended());
        t1.interrupt();
        awaitTermination(t1);
        assertTrue(sync.hasContended());
        sync.release();
        awaitTermination(t2);
        assertTrue(sync.hasContended());
    }

    /**
     * getQueuedThreads returns all waiting threads
     */
    @Test
    public void testGetQueuedThreads() {
        final Mutex sync = new Mutex();
        Thread t1 = new Thread(new InterruptedSyncRunnable(sync));
        Thread t2 = new Thread(new InterruptibleSyncRunnable(sync));
        assertHasExclusiveQueuedThreads(sync, NO_THREADS);
        sync.acquire();
        assertHasExclusiveQueuedThreads(sync, NO_THREADS);
        t1.start();
        waitForQueuedThread(sync, t1);
        assertHasExclusiveQueuedThreads(sync, t1);
        assertTrue(sync.getQueuedThreads().contains(t1));
        assertFalse(sync.getQueuedThreads().contains(t2));
        t2.start();
        waitForQueuedThread(sync, t2);
        assertHasExclusiveQueuedThreads(sync, t1, t2);
        assertTrue(sync.getQueuedThreads().contains(t1));
        assertTrue(sync.getQueuedThreads().contains(t2));
        t1.interrupt();
        awaitTermination(t1);
        assertHasExclusiveQueuedThreads(sync, t2);
        sync.release();
        awaitTermination(t2);
        assertHasExclusiveQueuedThreads(sync, NO_THREADS);
    }

    /**
     * getExclusiveQueuedThreads returns all exclusive waiting threads
     */
    @Test
    public void testGetExclusiveQueuedThreads() {
        final Mutex sync = new Mutex();
        Thread t1 = new Thread(new InterruptedSyncRunnable(sync));
        Thread t2 = new Thread(new InterruptibleSyncRunnable(sync));
        assertHasExclusiveQueuedThreads(sync, NO_THREADS);
        sync.acquire();
        assertHasExclusiveQueuedThreads(sync, NO_THREADS);
        t1.start();
        waitForQueuedThread(sync, t1);
        assertHasExclusiveQueuedThreads(sync, t1);
        assertTrue(sync.getExclusiveQueuedThreads().contains(t1));
        assertFalse(sync.getExclusiveQueuedThreads().contains(t2));
        t2.start();
        waitForQueuedThread(sync, t2);
        assertHasExclusiveQueuedThreads(sync, t1, t2);
        assertTrue(sync.getExclusiveQueuedThreads().contains(t1));
        assertTrue(sync.getExclusiveQueuedThreads().contains(t2));
        t1.interrupt();
        awaitTermination(t1);
        assertHasExclusiveQueuedThreads(sync, t2);
        sync.release();
        awaitTermination(t2);
        assertHasExclusiveQueuedThreads(sync, NO_THREADS);
    }

    /**
     * getSharedQueuedThreads does not include exclusively waiting threads
     */
    @Test
    public void testGetSharedQueuedThreads_Exclusive() {
        final Mutex sync = new Mutex();
        assertTrue(sync.getSharedQueuedThreads().isEmpty());
        sync.acquire();
        assertTrue(sync.getSharedQueuedThreads().isEmpty());
        Thread t1 = newStartedThread(new InterruptedSyncRunnable(sync));
        waitForQueuedThread(sync, t1);
        assertTrue(sync.getSharedQueuedThreads().isEmpty());
        Thread t2 = newStartedThread(new InterruptibleSyncRunnable(sync));
        waitForQueuedThread(sync, t2);
        assertTrue(sync.getSharedQueuedThreads().isEmpty());
        t1.interrupt();
        awaitTermination(t1);
        assertTrue(sync.getSharedQueuedThreads().isEmpty());
        sync.release();
        awaitTermination(t2);
        assertTrue(sync.getSharedQueuedThreads().isEmpty());
    }

    /**
     * getSharedQueuedThreads returns all shared waiting threads
     */
    @Test
    public void testGetSharedQueuedThreads_Shared() {
        final BooleanLatch l = new BooleanLatch();
        assertHasSharedQueuedThreads(l, NO_THREADS);
        Thread t1 = newStartedThread(new CheckedInterruptedRunnable() {
            public void realRun() throws InterruptedException {
                l.acquireSharedInterruptibly(0);
            }});
        waitForQueuedThread(l, t1);
        assertHasSharedQueuedThreads(l, t1);
        Thread t2 = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                l.acquireSharedInterruptibly(0);
            }});
        waitForQueuedThread(l, t2);
        assertHasSharedQueuedThreads(l, t1, t2);
        t1.interrupt();
        awaitTermination(t1);
        assertHasSharedQueuedThreads(l, t2);
        assertTrue(l.releaseShared(0));
        awaitTermination(t2);
        assertHasSharedQueuedThreads(l, NO_THREADS);
    }

    /**
     * tryAcquireNanos is interruptible
     */
//    @Test
    public void testTryAcquireNanos_Interruptible() {
        final Mutex sync = new Mutex();
        sync.acquire();
        Thread t = newStartedThread(new CheckedInterruptedRunnable() {
            public void realRun() throws InterruptedException {
                sync.tryAcquireNanos(MILLISECONDS.toNanos(2 * LONG_DELAY_MS));
            }});

        waitForQueuedThread(sync, t);
        t.interrupt();
        awaitTermination(t);
    }

    /**
     * tryAcquire on exclusively held sync fails
     */
    @Test
    public void testTryAcquireWhenSynced() {
        final Mutex sync = new Mutex();
        sync.acquire();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                assertFalse(sync.tryAcquire());
            }});

        awaitTermination(t);
        sync.release();
    }

    /**
     * tryAcquireNanos on an exclusively held sync times out
     */
    @Test
    public void testAcquireNanos_Timeout() {
        final Mutex sync = new Mutex();
        sync.acquire();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                long startTime = System.nanoTime();
                long nanos = MILLISECONDS.toNanos(timeoutMillis());
                assertFalse(sync.tryAcquireNanos(nanos));
                assertTrue(millisElapsedSince(startTime) >= timeoutMillis());
            }});

        awaitTermination(t);
        sync.release();
    }

    /**
     * getState is true when acquired and false when not
     */
    @Test
    public void testGetState() {
        final Mutex sync = new Mutex();
        sync.acquire();
        assertTrue(sync.isHeldExclusively());
        sync.release();
        assertFalse(sync.isHeldExclusively());

        final BooleanLatch acquired = new BooleanLatch();
        final BooleanLatch done = new BooleanLatch();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                sync.acquire();
                assertTrue(acquired.releaseShared(0));
                done.acquireShared(0);
                sync.release();
            }});

        acquired.acquireShared(0);
        assertTrue(sync.isHeldExclusively());
        assertTrue(done.releaseShared(0));
        awaitTermination(t);
        assertFalse(sync.isHeldExclusively());
    }

    /**
     * acquireInterruptibly succeeds when released, else is interruptible
     */
    @Test
    public void testAcquireInterruptibly() throws InterruptedException {
        final Mutex sync = new Mutex();
        final BooleanLatch threadStarted = new BooleanLatch();
        sync.acquireInterruptibly();
        Thread t = newStartedThread(new CheckedInterruptedRunnable() {
            public void realRun() throws InterruptedException {
                assertTrue(threadStarted.releaseShared(0));
                sync.acquireInterruptibly();
            }});

        threadStarted.acquireShared(0);
        waitForQueuedThread(sync, t);
        t.interrupt();
        awaitTermination(t);
        assertTrue(sync.isHeldExclusively());
    }

    /**
     * owns is true for a condition created by sync else false
     */
    @Test
    public void testOwns() {
        final Mutex sync = new Mutex();
        final AbstractQueuedSynchronizer.ConditionObject c = sync.newCondition();
        final Mutex sync2 = new Mutex();
        assertTrue(sync.owns(c));
        assertFalse(sync2.owns(c));
    }

    /**
     * Calling await without holding sync throws IllegalMonitorStateException
     */
//    @Test
    public void testAwait_IMSE() {
        final Mutex sync = new Mutex();
        final AbstractQueuedSynchronizer.ConditionObject c = sync.newCondition();
        for (AwaitMethod awaitMethod : AwaitMethod.values()) {
            long startTime = System.nanoTime();
            try {
                await(c, awaitMethod);
                shouldThrow();
            } catch (IllegalMonitorStateException success) {
            } catch (InterruptedException e) { threadUnexpectedException(e); }
            assertTrue(millisElapsedSince(startTime) < LONG_DELAY_MS);
        }
    }

    /**
     * Calling signal without holding sync throws IllegalMonitorStateException
     */
    @Test
    public void testSignal_IMSE() {
        final Mutex sync = new Mutex();
        final AbstractQueuedSynchronizer.ConditionObject c = sync.newCondition();
        try {
            c.signal();
            shouldThrow();
        } catch (IllegalMonitorStateException success) {}
        assertHasWaitersUnlocked(sync, c, NO_THREADS);
    }

    /**
     * Calling signalAll without holding sync throws IllegalMonitorStateException
     */
    @Test
    public void testSignalAll_IMSE() {
        final Mutex sync = new Mutex();
        final AbstractQueuedSynchronizer.ConditionObject c = sync.newCondition();
        try {
            c.signalAll();
            shouldThrow();
        } catch (IllegalMonitorStateException success) {}
    }

    /**
     * await/awaitNanos/awaitUntil without a signal times out
     */
    public void testAwaitTimed_Timeout() { testAwait_Timeout(AwaitMethod.awaitTimed); }
    public void testAwaitNanos_Timeout() { testAwait_Timeout(AwaitMethod.awaitNanos); }
    public void testAwaitUntil_Timeout() { testAwait_Timeout(AwaitMethod.awaitUntil); }
    public void testAwait_Timeout(AwaitMethod awaitMethod) {
        final Mutex sync = new Mutex();
        final AbstractQueuedSynchronizer.ConditionObject c = sync.newCondition();
        sync.acquire();
        assertAwaitTimesOut(c, awaitMethod);
        sync.release();
    }

    /**
     * await/awaitNanos/awaitUntil returns when signalled
     */
    @Test
    public void testSignal_await()      { testSignal(AwaitMethod.await); }
//    @Test
    public void testSignal_awaitTimed() { testSignal(AwaitMethod.awaitTimed); }
//    @Test
    public void testSignal_awaitNanos() { testSignal(AwaitMethod.awaitNanos); }
//    @Test
    public void testSignal_awaitUntil() { testSignal(AwaitMethod.awaitUntil); }
    public void testSignal(final AwaitMethod awaitMethod) {
        final Mutex sync = new Mutex();
        final AbstractQueuedSynchronizer.ConditionObject c = sync.newCondition();
        final BooleanLatch acquired = new BooleanLatch();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                sync.acquire();
                assertTrue(acquired.releaseShared(0));
                await(c, awaitMethod);
                sync.release();
            }});

        acquired.acquireShared(0);
        sync.acquire();
        assertHasWaitersLocked(sync, c, t);
        assertHasExclusiveQueuedThreads(sync, NO_THREADS);
        c.signal();
        assertHasWaitersLocked(sync, c, NO_THREADS);
        assertHasExclusiveQueuedThreads(sync, t);
        sync.release();
        awaitTermination(t);
    }

    /**
     * hasWaiters(null) throws NullPointerException
     */
    @Test
    public void testHasWaitersNPE() {
        final Mutex sync = new Mutex();
        try {
            sync.hasWaiters(null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * getWaitQueueLength(null) throws NullPointerException
     */
    @Test
    public void testGetWaitQueueLengthNPE() {
        final Mutex sync = new Mutex();
        try {
            sync.getWaitQueueLength(null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * getWaitingThreads throws NPE if null
     */
    @Test
    public void testGetWaitingThreadsNPE() {
        final Mutex sync = new Mutex();
        try {
            sync.getWaitingThreads(null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * hasWaiters throws IllegalArgumentException if not owned
     */
    @Test
    public void testHasWaitersIAE() {
        final Mutex sync = new Mutex();
        final AbstractQueuedSynchronizer.ConditionObject c = sync.newCondition();
        final Mutex sync2 = new Mutex();
        try {
            sync2.hasWaiters(c);
            shouldThrow();
        } catch (IllegalArgumentException success) {}
        assertHasWaitersUnlocked(sync, c, NO_THREADS);
    }

    /**
     * hasWaiters throws IllegalMonitorStateException if not synced
     */
    @Test
    public void testHasWaitersIMSE() {
        final Mutex sync = new Mutex();
        final AbstractQueuedSynchronizer.ConditionObject c = sync.newCondition();
        try {
            sync.hasWaiters(c);
            shouldThrow();
        } catch (IllegalMonitorStateException success) {}
        assertHasWaitersUnlocked(sync, c, NO_THREADS);
    }

    /**
     * getWaitQueueLength throws IllegalArgumentException if not owned
     */
    @Test
    public void testGetWaitQueueLengthIAE() {
        final Mutex sync = new Mutex();
        final AbstractQueuedSynchronizer.ConditionObject c = sync.newCondition();
        final Mutex sync2 = new Mutex();
        try {
            sync2.getWaitQueueLength(c);
            shouldThrow();
        } catch (IllegalArgumentException success) {}
        assertHasWaitersUnlocked(sync, c, NO_THREADS);
    }

    /**
     * getWaitQueueLength throws IllegalMonitorStateException if not synced
     */
    @Test
    public void testGetWaitQueueLengthIMSE() {
        final Mutex sync = new Mutex();
        final AbstractQueuedSynchronizer.ConditionObject c = sync.newCondition();
        try {
            sync.getWaitQueueLength(c);
            shouldThrow();
        } catch (IllegalMonitorStateException success) {}
        assertHasWaitersUnlocked(sync, c, NO_THREADS);
    }

    /**
     * getWaitingThreads throws IllegalArgumentException if not owned
     */
    @Test
    public void testGetWaitingThreadsIAE() {
        final Mutex sync = new Mutex();
        final AbstractQueuedSynchronizer.ConditionObject c = sync.newCondition();
        final Mutex sync2 = new Mutex();
        try {
            sync2.getWaitingThreads(c);
            shouldThrow();
        } catch (IllegalArgumentException success) {}
        assertHasWaitersUnlocked(sync, c, NO_THREADS);
    }

    /**
     * getWaitingThreads throws IllegalMonitorStateException if not synced
     */
    @Test
    public void testGetWaitingThreadsIMSE() {
        final Mutex sync = new Mutex();
        final AbstractQueuedSynchronizer.ConditionObject c = sync.newCondition();
        try {
            sync.getWaitingThreads(c);
            shouldThrow();
        } catch (IllegalMonitorStateException success) {}
        assertHasWaitersUnlocked(sync, c, NO_THREADS);
    }

    /**
     * hasWaiters returns true when a thread is waiting, else false
     */
    @Test
    public void testHasWaiters() {
        final Mutex sync = new Mutex();
        final AbstractQueuedSynchronizer.ConditionObject c = sync.newCondition();
        final BooleanLatch acquired = new BooleanLatch();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                sync.acquire();
                assertHasWaitersLocked(sync, c, NO_THREADS);
                assertFalse(sync.hasWaiters(c));
                assertTrue(acquired.releaseShared(0));
                c.await();
                sync.release();
            }});

        acquired.acquireShared(0);
        sync.acquire();
        assertHasWaitersLocked(sync, c, t);
        assertHasExclusiveQueuedThreads(sync, NO_THREADS);
        assertTrue(sync.hasWaiters(c));
        c.signal();
        assertHasWaitersLocked(sync, c, NO_THREADS);
        assertHasExclusiveQueuedThreads(sync, t);
        assertFalse(sync.hasWaiters(c));
        sync.release();

        awaitTermination(t);
        assertHasWaitersUnlocked(sync, c, NO_THREADS);
    }

    /**
     * getWaitQueueLength returns number of waiting threads
     */
    @Test
    public void testGetWaitQueueLength() {
        final Mutex sync = new Mutex();
        final AbstractQueuedSynchronizer.ConditionObject c = sync.newCondition();
        final BooleanLatch acquired1 = new BooleanLatch();
        final BooleanLatch acquired2 = new BooleanLatch();
        final Thread t1 = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                sync.acquire();
                assertHasWaitersLocked(sync, c, NO_THREADS);
                assertEquals(0, sync.getWaitQueueLength(c));
                assertTrue(acquired1.releaseShared(0));
                c.await();
                sync.release();
            }});
        acquired1.acquireShared(0);
        sync.acquire();
        assertHasWaitersLocked(sync, c, t1);
        assertEquals(1, sync.getWaitQueueLength(c));
        sync.release();

        final Thread t2 = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                sync.acquire();
                assertHasWaitersLocked(sync, c, t1);
                assertEquals(1, sync.getWaitQueueLength(c));
                assertTrue(acquired2.releaseShared(0));
                c.await();
                sync.release();
            }});
        acquired2.acquireShared(0);
        sync.acquire();
        assertHasWaitersLocked(sync, c, t1, t2);
        assertHasExclusiveQueuedThreads(sync, NO_THREADS);
        assertEquals(2, sync.getWaitQueueLength(c));
        c.signalAll();
        assertHasWaitersLocked(sync, c, NO_THREADS);
        assertHasExclusiveQueuedThreads(sync, t1, t2);
        assertEquals(0, sync.getWaitQueueLength(c));
        sync.release();

        awaitTermination(t1);
        awaitTermination(t2);
        assertHasWaitersUnlocked(sync, c, NO_THREADS);
    }

    /**
     * getWaitingThreads returns only and all waiting threads
     */
    @Test
    public void testGetWaitingThreads() {
        final Mutex sync = new Mutex();
        final AbstractQueuedSynchronizer.ConditionObject c = sync.newCondition();
        final BooleanLatch acquired1 = new BooleanLatch();
        final BooleanLatch acquired2 = new BooleanLatch();
        final Thread t1 = new Thread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                sync.acquire();
                assertHasWaitersLocked(sync, c, NO_THREADS);
                assertTrue(sync.getWaitingThreads(c).isEmpty());
                assertTrue(acquired1.releaseShared(0));
                c.await();
                sync.release();
            }});

        final Thread t2 = new Thread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                sync.acquire();
                assertHasWaitersLocked(sync, c, t1);
                assertTrue(sync.getWaitingThreads(c).contains(t1));
                assertFalse(sync.getWaitingThreads(c).isEmpty());
                assertEquals(1, sync.getWaitingThreads(c).size());
                assertTrue(acquired2.releaseShared(0));
                c.await();
                sync.release();
            }});

        sync.acquire();
        assertHasWaitersLocked(sync, c, NO_THREADS);
        assertFalse(sync.getWaitingThreads(c).contains(t1));
        assertFalse(sync.getWaitingThreads(c).contains(t2));
        assertTrue(sync.getWaitingThreads(c).isEmpty());
        assertEquals(0, sync.getWaitingThreads(c).size());
        sync.release();

        t1.start();
        acquired1.acquireShared(0);
        sync.acquire();
        assertHasWaitersLocked(sync, c, t1);
        assertTrue(sync.getWaitingThreads(c).contains(t1));
        assertFalse(sync.getWaitingThreads(c).contains(t2));
        assertFalse(sync.getWaitingThreads(c).isEmpty());
        assertEquals(1, sync.getWaitingThreads(c).size());
        sync.release();

        t2.start();
        acquired2.acquireShared(0);
        sync.acquire();
        assertHasWaitersLocked(sync, c, t1, t2);
        assertHasExclusiveQueuedThreads(sync, NO_THREADS);
        assertTrue(sync.getWaitingThreads(c).contains(t1));
        assertTrue(sync.getWaitingThreads(c).contains(t2));
        assertFalse(sync.getWaitingThreads(c).isEmpty());
        assertEquals(2, sync.getWaitingThreads(c).size());
        c.signalAll();
        assertHasWaitersLocked(sync, c, NO_THREADS);
        assertHasExclusiveQueuedThreads(sync, t1, t2);
        assertFalse(sync.getWaitingThreads(c).contains(t1));
        assertFalse(sync.getWaitingThreads(c).contains(t2));
        assertTrue(sync.getWaitingThreads(c).isEmpty());
        assertEquals(0, sync.getWaitingThreads(c).size());
        sync.release();

        awaitTermination(t1);
        awaitTermination(t2);
        assertHasWaitersUnlocked(sync, c, NO_THREADS);
    }

    /**
     * awaitUninterruptibly is uninterruptible
     */
    @Test
    public void testAwaitUninterruptibly() {
        final Mutex sync = new Mutex();
        final AbstractQueuedSynchronizer.ConditionObject c = sync.newCondition();
        final BooleanLatch pleaseInterrupt = new BooleanLatch();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                sync.acquire();
                assertTrue(pleaseInterrupt.releaseShared(0));
                c.awaitUninterruptibly();
                assertTrue(Thread.interrupted());
                assertHasWaitersLocked(sync, c, NO_THREADS);
                sync.release();
            }});

        pleaseInterrupt.acquireShared(0);
        sync.acquire();
        assertHasWaitersLocked(sync, c, t);
        sync.release();
        t.interrupt();
        assertHasWaitersUnlocked(sync, c, t);
        assertThreadStaysAlive(t);
        sync.acquire();
        assertHasWaitersLocked(sync, c, t);
        assertHasExclusiveQueuedThreads(sync, NO_THREADS);
        c.signal();
        assertHasWaitersLocked(sync, c, NO_THREADS);
        assertHasExclusiveQueuedThreads(sync, t);
        sync.release();
        awaitTermination(t);
    }

    /**
     * await/awaitNanos/awaitUntil is interruptible
     */
    @Test
    public void testInterruptible_await()      { testInterruptible(AwaitMethod.await); }
    @Test
    public void testInterruptible_awaitTimed() { testInterruptible(AwaitMethod.awaitTimed); }
    @Test
    public void testInterruptible_awaitNanos() { testInterruptible(AwaitMethod.awaitNanos); }
    @Test
    public void testInterruptible_awaitUntil() { testInterruptible(AwaitMethod.awaitUntil); }
    public void testInterruptible(final AwaitMethod awaitMethod) {
        final Mutex sync = new Mutex();
        final AbstractQueuedSynchronizer.ConditionObject c = sync.newCondition();
        final BooleanLatch pleaseInterrupt = new BooleanLatch();
        Thread t = newStartedThread(new CheckedInterruptedRunnable() {
            public void realRun() throws InterruptedException {
                sync.acquire();
                assertTrue(pleaseInterrupt.releaseShared(0));
                await(c, awaitMethod);
            }});

        pleaseInterrupt.acquireShared(0);
        t.interrupt();
        awaitTermination(t);
    }

    /**
     * signalAll wakes up all threads
     */
    @Test
    public void testSignalAll_await()      { testSignalAll(AwaitMethod.await); }
//    @Test
    public void testSignalAll_awaitTimed() { testSignalAll(AwaitMethod.awaitTimed); }
//    @Test
    public void testSignalAll_awaitNanos() { testSignalAll(AwaitMethod.awaitNanos); }
//    @Test
    public void testSignalAll_awaitUntil() { testSignalAll(AwaitMethod.awaitUntil); }
    public void testSignalAll(final AwaitMethod awaitMethod) {
        final Mutex sync = new Mutex();
        final AbstractQueuedSynchronizer.ConditionObject c = sync.newCondition();
        final BooleanLatch acquired1 = new BooleanLatch();
        final BooleanLatch acquired2 = new BooleanLatch();
        Thread t1 = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                sync.acquire();
                acquired1.releaseShared(0);
                await(c, awaitMethod);
                sync.release();
            }});

        Thread t2 = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                sync.acquire();
                acquired2.releaseShared(0);
                await(c, awaitMethod);
                sync.release();
            }});

        acquired1.acquireShared(0);
        acquired2.acquireShared(0);
        sync.acquire();
        assertHasWaitersLocked(sync, c, t1, t2);
        assertHasExclusiveQueuedThreads(sync, NO_THREADS);
        c.signalAll();
        assertHasWaitersLocked(sync, c, NO_THREADS);
        assertHasExclusiveQueuedThreads(sync, t1, t2);
        sync.release();
        awaitTermination(t1);
        awaitTermination(t2);
    }

    /**
     * toString indicates current state
     */
    @Test
    public void testToString() {
        Mutex sync = new Mutex();
        assertTrue(sync.toString().contains("State = " + Mutex.UNLOCKED));
        sync.acquire();
        assertTrue(sync.toString().contains("State = " + Mutex.LOCKED));
    }

    /**
     * A serialized AQS deserializes with current state, but no queued threads
     */
    @Test
    public void testSerialization() {
        Mutex sync = new Mutex();
        assertFalse(serialClone(sync).isHeldExclusively());
        sync.acquire();
        Thread t = newStartedThread(new InterruptedSyncRunnable(sync));
        waitForQueuedThread(sync, t);
        assertTrue(sync.isHeldExclusively());

        Mutex clone = serialClone(sync);
        assertTrue(clone.isHeldExclusively());
        assertHasExclusiveQueuedThreads(sync, t);
        assertHasExclusiveQueuedThreads(clone, NO_THREADS);
        t.interrupt();
        awaitTermination(t);
        sync.release();
        assertFalse(sync.isHeldExclusively());
        assertTrue(clone.isHeldExclusively());
        assertHasExclusiveQueuedThreads(sync, NO_THREADS);
        assertHasExclusiveQueuedThreads(clone, NO_THREADS);
    }

    /**
     * tryReleaseShared setting state changes getState
     */
    @Test
    public void testGetStateWithReleaseShared() {
        final BooleanLatch l = new BooleanLatch();
        assertFalse(l.isSignalled());
        assertTrue(l.releaseShared(0));
        assertTrue(l.isSignalled());
    }

    /**
     * releaseShared has no effect when already signalled
     */
    @Test
    public void testReleaseShared() {
        final BooleanLatch l = new BooleanLatch();
        assertFalse(l.isSignalled());
        assertTrue(l.releaseShared(0));
        assertTrue(l.isSignalled());
        assertTrue(l.releaseShared(0));
        assertTrue(l.isSignalled());
    }

    /**
     * acquireSharedInterruptibly returns after release, but not before
     */
    @Test
    public void testAcquireSharedInterruptibly() {
        final BooleanLatch l = new BooleanLatch();

        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                assertFalse(l.isSignalled());
                l.acquireSharedInterruptibly(0);
                assertTrue(l.isSignalled());
                l.acquireSharedInterruptibly(0);
                assertTrue(l.isSignalled());
            }});

        waitForQueuedThread(l, t);
        assertFalse(l.isSignalled());
        assertThreadStaysAlive(t);
        assertHasSharedQueuedThreads(l, t);
        assertTrue(l.releaseShared(0));
        assertTrue(l.isSignalled());
        awaitTermination(t);
    }

    /**
     * tryAcquireSharedNanos returns after release, but not before
     */
//    @Test
    public void testTryAcquireSharedNanos() {
        final BooleanLatch l = new BooleanLatch();

        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                assertFalse(l.isSignalled());
                long nanos = MILLISECONDS.toNanos(2 * LONG_DELAY_MS);
                assertTrue(l.tryAcquireSharedNanos(0, nanos));
                assertTrue(l.isSignalled());
                assertTrue(l.tryAcquireSharedNanos(0, nanos));
                assertTrue(l.isSignalled());
            }});

        waitForQueuedThread(l, t);
        assertFalse(l.isSignalled());
        assertThreadStaysAlive(t);
        assertTrue(l.releaseShared(0));
        assertTrue(l.isSignalled());
        awaitTermination(t);
    }

    /**
     * acquireSharedInterruptibly is interruptible
     */
    @Test
    public void testAcquireSharedInterruptibly_Interruptible() {
        final BooleanLatch l = new BooleanLatch();
        Thread t = newStartedThread(new CheckedInterruptedRunnable() {
            public void realRun() throws InterruptedException {
                assertFalse(l.isSignalled());
                l.acquireSharedInterruptibly(0);
            }});

        waitForQueuedThread(l, t);
        assertFalse(l.isSignalled());
        t.interrupt();
        awaitTermination(t);
        assertFalse(l.isSignalled());
    }

    /**
     * tryAcquireSharedNanos is interruptible
     */
//    @Test
    public void testTryAcquireSharedNanos_Interruptible() {
        final BooleanLatch l = new BooleanLatch();
        Thread t = newStartedThread(new CheckedInterruptedRunnable() {
            public void realRun() throws InterruptedException {
                assertFalse(l.isSignalled());
                long nanos = MILLISECONDS.toNanos(2 * LONG_DELAY_MS);
                l.tryAcquireSharedNanos(0, nanos);
            }});

        waitForQueuedThread(l, t);
        assertFalse(l.isSignalled());
        t.interrupt();
        awaitTermination(t);
        assertFalse(l.isSignalled());
    }

    /**
     * tryAcquireSharedNanos times out if not released before timeout
     */
//    @Test
    public void testTryAcquireSharedNanos_Timeout() {
        final BooleanLatch l = new BooleanLatch();
        final BooleanLatch observedQueued = new BooleanLatch();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                assertFalse(l.isSignalled());
                for (long millis = timeoutMillis();
                     !observedQueued.isSignalled();
                     millis *= 2) {
                    long nanos = MILLISECONDS.toNanos(millis);
                    long startTime = System.nanoTime();
                    assertFalse(l.tryAcquireSharedNanos(0, nanos));
                    assertTrue(millisElapsedSince(startTime) >= millis);
                }
                assertFalse(l.isSignalled());
            }});

        waitForQueuedThread(l, t);
        observedQueued.releaseShared(0);
        assertFalse(l.isSignalled());
        awaitTermination(t);
        assertFalse(l.isSignalled());
    }

    /**
     * awaitNanos/timed await with 0 wait times out immediately
     */
    @Test
    public void testAwait_Zero() throws InterruptedException {
        final Mutex sync = new Mutex();
        final AbstractQueuedSynchronizer.ConditionObject c = sync.newCondition();
        sync.acquire();
        assertTrue(c.awaitNanos(0L) <= 0);
        assertFalse(c.await(0L, NANOSECONDS));
        sync.release();
    }

    /**
     * awaitNanos/timed await with maximum negative wait times does not underflow
     */
//    @Test
    public void testAwait_NegativeInfinity() throws InterruptedException {
        final Mutex sync = new Mutex();
        final AbstractQueuedSynchronizer.ConditionObject c = sync.newCondition();
        sync.acquire();
        assertTrue(c.awaitNanos(Long.MIN_VALUE) <= 0);
        assertFalse(c.await(Long.MIN_VALUE, NANOSECONDS));
        sync.release();
    }
}
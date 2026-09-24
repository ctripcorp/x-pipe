package com.ctrip.xpipe.redis.keeper.storage;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * ExecutorService wrapper that tracks all submitted tasks for test synchronization.
 * <p>
 * Usage:
 * <pre>
 *   TrackingExecutor tracking = new TrackingExecutor(Executors.newCachedThreadPool());
 *   // ... submit tasks via TCF or delegate ...
 *   int count = tracking.submittedCount(); // check how many tasks submitted
 *   tracking.awaitAll();                   // wait for all to complete
 * </pre>
 */
public class TrackingExecutor implements ExecutorService {

    private final ExecutorService delegate;
    private final List<Future<?>> submitted = new CopyOnWriteArrayList<>();

    public TrackingExecutor(ExecutorService delegate) {
        this.delegate = delegate;
    }

    /**
     * @return total number of tasks submitted so far (never decreases)
     */
    public int submittedCount() {
        return submitted.size();
    }

    /**
     * Clear all tracked futures. Call between benchmark phases so that
     * awaitAll() only waits for tasks submitted in the current phase.
     */
    public void clear() {
        submitted.clear();
    }

    /**
     * Wait for all tracked tasks then clear. Swallows exceptions like awaitAll().
     * Suitable for use as a Runnable (e.g. method reference in benchmarks).
     */
    public void awaitAndClear() {
        try {
            awaitAll();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        clear();
    }

    /**
     * Wait for all currently submitted tasks to complete.
     * Takes a snapshot and waits for each future. Exceptions are swallowed
     * (failed tasks like EIO are expected in tests).
     */
    public void awaitAll() throws Exception {
        Future<?>[] snapshot = submitted.toArray(new Future<?>[0]);
        for (Future<?> f : snapshot) {
            try {
                f.get();
            } catch (ExecutionException e) {
                // ignore — task may have failed (e.g., EIO)
            }
        }
    }

    // ---- ExecutorService delegation ----

    @Override
    public void execute(Runnable command) {
        submitted.add(delegate.submit(command));
    }

    @Override
    public Future<?> submit(Runnable task) {
        Future<?> f = delegate.submit(task);
        submitted.add(f);
        return f;
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        Future<T> f = delegate.submit(task, result);
        submitted.add(f);
        return f;
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        Future<T> f = delegate.submit(task);
        submitted.add(f);
        return f;
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }

    @Override
    public <T> List<Future<T>> invokeAll(java.util.Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return delegate.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(java.util.Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(java.util.Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return delegate.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(java.util.Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return delegate.invokeAny(tasks, timeout, unit);
    }
}

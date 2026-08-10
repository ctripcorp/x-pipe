package com.ctrip.xpipe.redis.keeper.storage;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class AsyncFileSystemHelper {

    private static final Logger logger = LoggerFactory.getLogger(AsyncFileSystemHelper.class);

    public static final String EVENT_TYPE = "AsyncFileSystemCall";

    /** Helper-side cases (not thrown by AsyncFileSystem itself). */
    public static final String CASE_TIMEOUT = "Timeout";
    public static final String CASE_INTERRUPTED = "Interrupted";
    public static final String CASE_SHORT_WRITE = "ShortWrite";
    public static final String CASE_SHORT_READ = "ShortRead";
    public static final String CASE_ABANDON_CLOSE = "AbandonClose";
    public static final String CASE_ABANDON_CLOSE_FAIL = "AbandonCloseFail";
    public static final String CASE_CLOSE_NOT_EXECUTED = "CloseNotExecuted";
    public static final String CASE_CLOSE_TIMEOUT = "CloseTimeout";
    public static final String CASE_CLOSE_REJECTED = "CloseRejected";
    public static final String CASE_CLOSE_FAIL = "CloseFail";
    /** Phase H1: OperationNotExecutedException retried once at await entry. */
    public static final String CASE_NOT_EXECUTED_RETRY = "NotExecutedRetry";
    public static final String CASE_NOT_EXECUTED = "NotExecuted";

    public static final long DEFAULT_IO_TIMEOUT_MILLIS = 1000L;

    /**
     * Per-await budget for PREPARE lease release (Metaserver {@code TFS_STEP_TIMEOUT_MILLI=1000}).
     */
    public static final long PREPARE_IO_TIMEOUT_MILLIS = 250L;

    private static final ThreadLocal<Long> IO_TIMEOUT_MILLIS = new ThreadLocal<>();

    @FunctionalInterface
    public interface IoRunnable {
        void run() throws IOException;
    }

    /**
     * FS call that returns a future. Used so {@link #await(FsCall, String)} can re-invoke on
     * {@link OperationNotExecutedException} (often thrown synchronously before a future exists).
     */
    @FunctionalInterface
    public interface FsCall<T> {
        CompletableFuture<T> call();
    }

    private AsyncFileSystemHelper() {
    }

    /**
     * Run {@code action} with a tighter per-{@link #await} timeout on the current thread.
     */
    public static void runWithIoTimeout(long timeoutMillis, IoRunnable action) throws IOException {
        Long previous = IO_TIMEOUT_MILLIS.get();
        IO_TIMEOUT_MILLIS.set(timeoutMillis);
        try {
            action.run();
        } finally {
            if (previous == null) {
                IO_TIMEOUT_MILLIS.remove();
            } else {
                IO_TIMEOUT_MILLIS.set(previous);
            }
        }
    }

    /**
     * Invoke FS op and await. On {@link OperationNotExecutedException} (sync from FS, or as cause of
     * a failed future), <b>retry the op once</b>; still failing → rethrow ONE as-is (Phase H1 / AC-H).
     * Prefer this overload over {@link #await(CompletableFuture, String)} so sync ONE can be retried.
     */
    public static <T> T await(FsCall<T> call, String operation) throws IOException {
        try {
            return awaitFuture(call.call(), operation);
        } catch (OperationNotExecutedException first) {
            logger.warn("[await][not-executed][retry] {}", operation, first);
            logCase(CASE_NOT_EXECUTED_RETRY);
            try {
                return awaitFuture(call.call(), operation);
            } catch (OperationNotExecutedException second) {
                logCase(CASE_NOT_EXECUTED);
                throw second;
            }
        }
    }

    /**
     * Await an already-started future (no re-invoke). Prefer {@link #await(FsCall, String)} when the
     * FS call may throw {@link OperationNotExecutedException} synchronously.
     */
    public static <T> T await(CompletableFuture<T> future, String operation) throws IOException {
        return awaitFuture(future, operation);
    }

    public static <T> T await(CompletableFuture<T> future, String operation, long timeout, TimeUnit unit)
            throws IOException {
        return doAwait(future, operation, timeout, unit, () -> future.cancel(false));
    }

    /**
     * Await an {@code open} future. On timeout <b>or interrupt</b>, abandon: a late successful open
     * is best-effort {@link #closeReadHandle}'d (no await) so the handle is never handed to the caller
     * (avoids sticky {@code writer already open}). Timeout/interrupt still surfaces as {@link IOException}.
     * <p>
     * On {@link OperationNotExecutedException}, retries open once (same as {@link #await(FsCall, String)}).
     * <p>
     * Does <b>not</b> {@code cancel} the open future: {@link CompletableFuture#cancel} on a
     * {@code supplyAsync} future discards the result while the IO task may still finish and hold
     * the write slot — then abandon cannot reclaim the handle.
     * <p>
     * Timeout budget follows {@link #await} / {@link #runWithIoTimeout}.
     */
    public static <T extends AbstractStorageFile> T awaitOpen(AsyncFileSystem fs, CompletableFuture<T> future,
                                                              String operation) throws IOException {
        // Already-started future: cannot re-invoke; ONE sync would have escaped before this call.
        return doAwait(future, operation, currentTimeoutMillis(), TimeUnit.MILLISECONDS,
                () -> abandonOpen(fs, future, operation));
    }

    /**
     * Open via supplier so sync {@link OperationNotExecutedException} can be retried once.
     */
    public static <T extends AbstractStorageFile> T awaitOpen(AsyncFileSystem fs, FsCall<T> openCall,
                                                              String operation) throws IOException {
        try {
            return awaitOpen(fs, openCall.call(), operation);
        } catch (OperationNotExecutedException first) {
            logger.warn("[awaitOpen][not-executed][retry] {}", operation, first);
            logCase(CASE_NOT_EXECUTED_RETRY);
            try {
                return awaitOpen(fs, openCall.call(), operation);
            } catch (OperationNotExecutedException second) {
                logCase(CASE_NOT_EXECUTED);
                throw second;
            }
        }
    }

    /**
     * Best-effort close for <b>write</b> handles (spec §3.9.4b).
     * <ul>
     *   <li>{@link OperationNotExecutedException} (sync, before future): retry close once; still fail → skip</li>
     *   <li>Returned future: await with the same IO timeout as {@link #await}; on timeout do <b>not</b>
     *       cancel — hang {@code whenComplete} to log late failure</li>
     *   <li>Future completed with {@link RejectedExecutionException}: retry close once / skip</li>
     * </ul>
     * Never throws to caller (lease / Lifecycle preferred over perfect close).
     */
    public static void closeHandle(AsyncFileSystem fs, AbstractStorageFile file, String operation) {
        if (fs == null || file == null) {
            return;
        }
        CompletableFuture<Void> closeFuture = invokeCloseWithOneRetry(fs, file, operation);
        if (closeFuture == null) {
            return;
        }
        awaitCloseFuture(fs, file, closeFuture, operation, true);
    }

    /**
     * Best-effort close for <b>read</b> handles and abandon late-close (spec §3.9.4b).
     * Invokes close (with one {@link OperationNotExecutedException} retry) then observes the future
     * via {@code whenComplete} — <b>does not await</b>. Never throws.
     */
    public static void closeReadHandle(AsyncFileSystem fs, AbstractStorageFile file, String operation) {
        closeWithoutAwait(fs, file, operation, CloseObserveMode.READ);
    }

    private static long currentTimeoutMillis() {
        Long override = IO_TIMEOUT_MILLIS.get();
        return override != null ? override : DEFAULT_IO_TIMEOUT_MILLIS;
    }

    @FunctionalInterface
    private interface OnTimeout {
        void accept();
    }

    private static <T> T awaitFuture(CompletableFuture<T> future, String operation) throws IOException {
        return doAwait(future, operation, currentTimeoutMillis(), TimeUnit.MILLISECONDS,
                () -> future.cancel(false));
    }

    private static <T> T doAwait(CompletableFuture<T> future, String operation, long timeout, TimeUnit unit,
                                 OnTimeout onTimeout) throws IOException {
        try {
            return future.get(timeout, unit);
        } catch (InterruptedException e) {
            // Same reclaim path as TimeoutException (Important #2 / T-S.8): awaitOpen→abandon; await→cancel.
            onTimeout.accept();
            Thread.currentThread().interrupt();
            logCase(CASE_INTERRUPTED);
            throw new IOException("interrupted while waiting async file IO: " + operation, e);
        } catch (TimeoutException e) {
            onTimeout.accept();
            logCase(CASE_TIMEOUT);
            throw new IOException("timeout waiting async file IO: " + operation, e);
        } catch (ExecutionException e) {
            // FS / IO-pool failure completed the future exceptionally.
            // RejectedExecutionException (queue full) lands here via failedFuture, not as a direct throw from get().
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            OperationNotExecutedException one = findOperationNotExecuted(cause);
            if (one != null) {
                // Surface as-is so {@link #await(FsCall, String)} can retry / callers see ONE.
                logFsException(one);
                throw one;
            }
            logFsException(cause);
            throw toIoException(operation, cause);
        } catch (OperationNotExecutedException e) {
            throw e;
        } catch (Throwable t) {
            // Safety net for anything get() throws outside the three above (e.g. CancellationException).
            logFsException(t);
            if (t instanceof Error) {
                throw (Error) t;
            }
            throw new IOException("unexpected async file IO failure: " + operation, t);
        }
    }

    private static OperationNotExecutedException findOperationNotExecuted(Throwable t) {
        while (t != null) {
            if (t instanceof OperationNotExecutedException) {
                return (OperationNotExecutedException) t;
            }
            t = t.getCause();
        }
        return null;
    }

    private static <T extends AbstractStorageFile> void abandonOpen(AsyncFileSystem fs, CompletableFuture<T> future,
                                                                    String operation) {
        // whenCompleteAsync: hop off the open-completing thread (often the IO pool) before invoking close,
        // which may sync-flush on the caller; never await close on that path.
        future.whenCompleteAsync((file, err) -> {
            if (err != null) {
                logger.info("[awaitOpen][abandon][late-fail] {}", operation, err);
                return;
            }
            if (file == null) {
                return;
            }
            closeWithoutAwait(fs, file, operation, CloseObserveMode.ABANDON);
        });
    }

    private enum CloseObserveMode {
        READ,
        ABANDON
    }

    private static void closeWithoutAwait(AsyncFileSystem fs, AbstractStorageFile file, String operation,
                                          CloseObserveMode mode) {
        if (fs == null || file == null) {
            return;
        }
        CompletableFuture<Void> closeFuture = invokeCloseWithOneRetry(fs, file, operation);
        if (closeFuture == null) {
            if (mode == CloseObserveMode.ABANDON) {
                logCase(CASE_ABANDON_CLOSE_FAIL);
                logger.warn("[awaitOpen][abandon][close-skip] {}", operation);
            }
            return;
        }
        observeCloseFuture(fs, file, closeFuture, operation, mode, true);
    }

    private static void observeCloseFuture(AsyncFileSystem fs, AbstractStorageFile file,
                                           CompletableFuture<Void> closeFuture, String operation,
                                           CloseObserveMode mode, boolean allowRejectedRetry) {
        closeFuture.whenComplete((ignored, err) -> {
            if (err == null) {
                if (mode == CloseObserveMode.ABANDON) {
                    logCase(CASE_ABANDON_CLOSE);
                    logger.info("[awaitOpen][abandon][closed] {}", operation);
                } else {
                    logger.debug("[closeReadHandle][closed] {}", operation);
                }
                return;
            }
            if (allowRejectedRetry && isRejectedExecution(err)) {
                logCase(CASE_CLOSE_REJECTED);
                logger.warn("[closeReadHandle][rejected][retry] {}", operation, err);
                CompletableFuture<Void> retryFuture = invokeCloseWithOneRetry(fs, file, operation);
                if (retryFuture == null) {
                    if (mode == CloseObserveMode.ABANDON) {
                        logCase(CASE_ABANDON_CLOSE_FAIL);
                        logger.warn("[awaitOpen][abandon][close-skip] {}", operation);
                    }
                    return;
                }
                observeCloseFuture(fs, file, retryFuture, operation, mode, false);
                return;
            }
            if (mode == CloseObserveMode.ABANDON) {
                logCase(CASE_ABANDON_CLOSE_FAIL);
                logger.error("[awaitOpen][abandon][close-fail] {}", operation, err);
            } else {
                logCase(CASE_CLOSE_FAIL);
                logger.error("[closeReadHandle][fail] {}", operation, err);
            }
        });
    }

    private static CompletableFuture<Void> invokeCloseWithOneRetry(AsyncFileSystem fs, AbstractStorageFile file,
                                                                   String operation) {
        try {
            return doFsClose(fs, file);
        } catch (OperationNotExecutedException first) {
            logger.warn("[closeHandle][not-executed][retry] {}", operation, first);
            try {
                return doFsClose(fs, file);
            } catch (OperationNotExecutedException second) {
                logCase(CASE_CLOSE_NOT_EXECUTED);
                logger.warn("[closeHandle][not-executed][skip] {}", operation, second);
                return null;
            } catch (Throwable t) {
                logCase(CASE_CLOSE_FAIL);
                logger.error("[closeHandle][retry-fail] {}", operation, t);
                return null;
            }
        } catch (Throwable t) {
            logCase(CASE_CLOSE_FAIL);
            logger.error("[closeHandle][sync-fail] {}", operation, t);
            return null;
        }
    }

    private static CompletableFuture<Void> doFsClose(AsyncFileSystem fs, AbstractStorageFile file) {
        if (file instanceof AsyncFile) {
            return fs.close((AsyncFile) file);
        }
        if (file instanceof AsyncSegmentFile) {
            return fs.close((AsyncSegmentFile) file);
        }
        throw new IllegalArgumentException("unsupported storage file type: " + file.getClass().getName());
    }

    private static void awaitCloseFuture(AsyncFileSystem fs, AbstractStorageFile file,
                                         CompletableFuture<Void> closeFuture, String operation,
                                         boolean allowRejectedRetry) {
        try {
            closeFuture.get(currentTimeoutMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            logCase(CASE_CLOSE_TIMEOUT);
            logger.warn("[closeHandle][timeout] {}", operation);
            // Do not cancel: TailCache may still release the write slot when the task runs.
            closeFuture.whenComplete((ignored, err) -> {
                if (err != null) {
                    logCase(CASE_CLOSE_FAIL);
                    logger.error("[closeHandle][late-fail] {}", operation, err);
                }
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logCase(CASE_INTERRUPTED);
            logger.warn("[closeHandle][interrupted] {}", operation);
            closeFuture.whenComplete((ignored, err) -> {
                if (err != null) {
                    logCase(CASE_CLOSE_FAIL);
                    logger.error("[closeHandle][late-fail] {}", operation, err);
                }
            });
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (allowRejectedRetry && isRejectedExecution(cause)) {
                logCase(CASE_CLOSE_REJECTED);
                logger.warn("[closeHandle][rejected][retry] {}", operation, cause);
                CompletableFuture<Void> retryFuture = invokeCloseWithOneRetry(fs, file, operation);
                if (retryFuture != null) {
                    // Second await: no further RejectedExecution retry (avoid loops).
                    awaitCloseFuture(fs, file, retryFuture, operation, false);
                }
                return;
            }
            logCase(CASE_CLOSE_FAIL);
            logger.error("[closeHandle][fail] {}", operation, cause);
        } catch (Throwable t) {
            logCase(CASE_CLOSE_FAIL);
            logger.error("[closeHandle][unexpected] {}", operation, t);
        }
    }

    private static boolean isRejectedExecution(Throwable t) {
        while (t != null) {
            if (t instanceof RejectedExecutionException) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    private static IOException toIoException(String operation, Throwable cause) throws Error {
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        if (cause instanceof StorageIOException && cause.getCause() instanceof IOException) {
            return (IOException) cause.getCause();
        }
        if (cause instanceof IOException) {
            return (IOException) cause;
        }
        return new IOException("async file IO failed: " + operation, cause);
    }

    public static void writeAllBytes(AsyncFileSystem fs, AsyncFile file, byte[] data, String operation)
            throws IOException {
        // Recreate ByteBuf each attempt: TailCache releases the buffer when throwing ONE.
        for (int attempt = 0; attempt < 2; attempt++) {
            ByteBuf buf = Unpooled.wrappedBuffer(data);
            buf.retain();
            try {
                long written = awaitFuture(fs.write(file, buf), operation);
                if (written != data.length) {
                    logCase(CASE_SHORT_WRITE);
                    throw new IOException("short async write, expected " + data.length + " but wrote " + written
                            + ": " + operation);
                }
                return;
            } catch (OperationNotExecutedException e) {
                if (attempt == 0) {
                    logger.warn("[writeAllBytes][not-executed][retry] {}", operation, e);
                    logCase(CASE_NOT_EXECUTED_RETRY);
                    continue;
                }
                logCase(CASE_NOT_EXECUTED);
                throw e;
            } finally {
                buf.release();
            }
        }
    }

    public static byte[] readAllBytes(AsyncFileSystem fs, AsyncFile file, long size, long offset, String operation)
            throws IOException {
        ByteBuf buf = await(() -> fs.read(file, size, offset), operation);
        try {
            if (buf.readableBytes() != size) {
                logCase(CASE_SHORT_READ);
                throw new IOException("failed to read full async file: " + operation
                        + ", expected " + size + " but got " + buf.readableBytes());
            }
            byte[] data = new byte[buf.readableBytes()];
            buf.readBytes(data);
            return data;
        } finally {
            buf.release();
        }
    }

    public static String readAllUtf8(AsyncFileSystem fs, AsyncFile file, long size, long offset, String operation)
            throws IOException {
        return readAllUtf8(fs, file, size, offset, StandardCharsets.UTF_8, operation);
    }

    public static String readAllUtf8(AsyncFileSystem fs, AsyncFile file, long size, long offset, Charset charset,
                                     String operation) throws IOException {
        byte[] data = readAllBytes(fs, file, size, offset, operation);
        return new String(data, charset);
    }

    public static long writeAndAwait(AsyncFileSystem fs, AsyncSegmentFile file, ByteBuf data, int expectedLength,
                                     String operation) throws IOException {
        return writeAndAwaitInternal(data, expectedLength, operation,
                buf -> fs.write(file, buf));
    }

    public static long writeAndAwait(AsyncFileSystem fs, AsyncFile file, ByteBuf data, int expectedLength,
                                     String operation) throws IOException {
        return writeAndAwaitInternal(data, expectedLength, operation,
                buf -> fs.write(file, buf));
    }

    /**
     * Write with ONE retry. Extra {@code retain} so TailCache {@code release} on ONE leaves the buffer usable.
     */
    private static long writeAndAwaitInternal(ByteBuf data, int expectedLength, String operation,
                                              FsCallFromBuf writeCall) throws IOException {
        for (int attempt = 0; attempt < 2; attempt++) {
            data.retain();
            try {
                long flushed = awaitFuture(writeCall.call(data), operation);
                if (flushed != expectedLength) {
                    logCase(CASE_SHORT_WRITE);
                    throw new IOException("short async write, expected " + expectedLength + " but flushed " + flushed
                            + ": " + operation);
                }
                // Success: FS owns/releases the write ref; drop our guard retain.
                data.release();
                return flushed;
            } catch (OperationNotExecutedException e) {
                // FS released one ref on ONE; our retain keeps the buffer alive for retry / exit.
                if (attempt == 0) {
                    logger.warn("[writeAndAwait][not-executed][retry] {}", operation, e);
                    logCase(CASE_NOT_EXECUTED_RETRY);
                    continue;
                }
                data.release();
                logCase(CASE_NOT_EXECUTED);
                throw e;
            } catch (IOException | RuntimeException | Error e) {
                data.release();
                throw e;
            }
        }
        throw new IllegalStateException("unreachable writeAndAwait retry loop: " + operation);
    }

    @FunctionalInterface
    private interface FsCallFromBuf {
        CompletableFuture<Long> call(ByteBuf data);
    }

    private static void logFsException(Throwable t) {
        EventMonitor.DEFAULT.logEvent(EVENT_TYPE, t.getClass().getSimpleName());
    }

    private static void logCase(String caseName) {
        EventMonitor.DEFAULT.logEvent(EVENT_TYPE, caseName);
    }
}

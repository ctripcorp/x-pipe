package com.ctrip.xpipe.redis.keeper.storage;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.core.store.ReplId;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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
    /** keeper-tfs-m6: a failed close handed to {@link DeferredHandleCloser}. */
    public static final String CASE_CLOSE_DEFERRED = "CloseDeferred";
    /** Phase H1: OperationNotExecutedException retried once at await entry. */
    public static final String CASE_NOT_EXECUTED_RETRY = "NotExecutedRetry";
    public static final String CASE_NOT_EXECUTED = "NotExecuted";
    public static final String CASE_MEMORY_RESERVE_FAIL = "MemoryReserveFail";
    public static final String CASE_UNKNOWN_FAIL = "UnknownFail";

    public static final long DEFAULT_IO_TIMEOUT_MILLIS = 1000L;

    /** D7: total write invocations including the original (retry ×1 ⇒ 2). Loop always calls write on each attempt. */
    private static final int WRITE_SYNC_THROW_MAX_ATTEMPTS = 2;

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
     * Invoke FS op and await. On synchronous {@link OperationNotExecutedException} (thrown before a
     * future is returned), <b>retry the op once</b>; still failing → rethrow ONE as-is (Phase H1 / AC-H).
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
     * Close for <b>write</b> handles (spec §3.9.4b, keeper-tfs-m6 §2.2).
     * <ul>
     *   <li>{@link OperationNotExecutedException} (sync, before future): retry close once</li>
     *   <li>Returned future: await with the same IO timeout as {@link #await}; on timeout / interrupt do
     *       <b>not</b> cancel — a late failure is deferred once the close has finished</li>
     *   <li>Future completed with {@link RejectedExecutionException}: retry close once</li>
     *   <li>Any other failure: the handle goes to the FS's {@link DeferredHandleCloser}, which owns it from
     *       then on; {@link IllegalStateException} / {@link IllegalArgumentException} are programming errors
     *       and are not deferred; an FS without a closer keeps the old skip</li>
     * </ul>
     * Never throws to caller (lease / Lifecycle preferred over perfect close).
     *
     * @param replId keeper the handle belongs to, for the deferred closer; may be null
     * @return true when closed (or nothing to close); false when the close did not complete: deferred,
     * skipped, still pending in the closer, or timed out / interrupted
     */
    public static boolean closeHandle(AsyncFileSystem fs, AbstractStorageFile file, ReplId replId,
                                      String operation) {
        if (fs == null || file == null) {
            return true;
        }
        CompletableFuture<Void> closeFuture = invokeCloseWithOneRetry(fs, file, replId, operation);
        if (closeFuture == null) {
            return false;
        }
        return awaitCloseFuture(fs, file, replId, closeFuture, operation, true);
    }

    /**
     * Close for <b>read</b> handles (spec §3.9.4b). Invokes close (with one
     * {@link OperationNotExecutedException} retry) then observes the future via {@code whenComplete} —
     * <b>does not await</b>. Failures are deferred as in {@link #closeHandle}. Never throws.
     */
    public static void closeReadHandle(AsyncFileSystem fs, AbstractStorageFile file, ReplId replId,
                                       String operation) {
        closeWithoutAwait(fs, file, replId, operation, CloseObserveMode.READ);
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
            // OperationNotExecutedException is only thrown synchronously before a future exists — not as get() cause.
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            logFsException(cause);
            throw toIoException(operation, cause);
        } catch (Throwable t) {
            // Safety net for anything get() throws outside the three above (e.g. CancellationException).
            logFsException(t);
            if (t instanceof Error) {
                throw (Error) t;
            }
            throw new IOException("unexpected async file IO failure: " + operation, t);
        }
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
            // keeper-tfs-m6 T-P2.0: the open entry has no ReplId until P5, so the closer shows "unknown".
            closeWithoutAwait(fs, file, null, operation, CloseObserveMode.ABANDON);
        });
    }

    private enum CloseObserveMode {
        READ,
        ABANDON
    }

    private static void closeWithoutAwait(AsyncFileSystem fs, AbstractStorageFile file, ReplId replId,
                                          String operation, CloseObserveMode mode) {
        if (fs == null || file == null) {
            return;
        }
        CompletableFuture<Void> closeFuture = invokeCloseWithOneRetry(fs, file, replId, operation);
        if (closeFuture == null) {
            if (mode == CloseObserveMode.ABANDON) {
                logCase(CASE_ABANDON_CLOSE_FAIL);
                logger.warn("[awaitOpen][abandon][close-not-done] {}", operation);
            }
            return;
        }
        observeCloseFuture(fs, file, replId, closeFuture, operation, mode, true);
    }

    private static void observeCloseFuture(AsyncFileSystem fs, AbstractStorageFile file, ReplId replId,
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
                CompletableFuture<Void> retryFuture = invokeCloseWithOneRetry(fs, file, replId, operation);
                if (retryFuture == null) {
                    if (mode == CloseObserveMode.ABANDON) {
                        logCase(CASE_ABANDON_CLOSE_FAIL);
                        logger.warn("[awaitOpen][abandon][close-not-done] {}", operation);
                    }
                    return;
                }
                observeCloseFuture(fs, file, replId, retryFuture, operation, mode, false);
                return;
            }
            if (mode == CloseObserveMode.ABANDON) {
                closeFailed(fs, file, replId, operation, "abandon-fail", CASE_ABANDON_CLOSE_FAIL, err);
            } else {
                closeFailed(fs, file, replId, operation, "read-fail", CASE_CLOSE_FAIL, err);
            }
        });
    }

    /**
     * Returns null when no close is in progress afterwards: the handle is still pending in the
     * deferred closer, or this close failed and went through {@link #closeFailed}.
     */
    private static CompletableFuture<Void> invokeCloseWithOneRetry(AsyncFileSystem fs, AbstractStorageFile file,
                                                                   ReplId replId, String operation) {
        DeferredHandleCloser closer = DeferredHandleCloser.of(fs);
        if (closer != null && closer.isPending(file)) {
            // Owned by the closer: a second close here would race its retry (double releaseEntry).
            logger.warn("[closeHandle][pending] repl_id={} {}", DeferredHandle.replIdText(replId), operation);
            return null;
        }
        try {
            return doFsClose(fs, file);
        } catch (OperationNotExecutedException first) {
            logger.warn("[closeHandle][not-executed][retry] {}", operation, first);
            try {
                return doFsClose(fs, file);
            } catch (OperationNotExecutedException second) {
                closeFailed(fs, file, replId, operation, "not-executed", CASE_CLOSE_NOT_EXECUTED, second);
                return null;
            } catch (Throwable t) {
                closeFailed(fs, file, replId, operation, "retry-fail", CASE_CLOSE_FAIL, t);
                return null;
            }
        } catch (Throwable t) {
            closeFailed(fs, file, replId, operation, "sync-fail", CASE_CLOSE_FAIL, t);
            return null;
        }
    }

    /**
     * Terminal close failure (keeper-tfs-m6 §2.2): hand the handle to the FS's deferred closer, which
     * keeps retrying until the cache entry is released. Only called once no close of {@code file} is
     * running, so the closer gets sole ownership.
     */
    private static void closeFailed(AsyncFileSystem fs, AbstractStorageFile file, ReplId replId, String operation,
                                    String reason, String eventCase, Throwable failure) {
        logCase(eventCase);
        Throwable cause = failure instanceof CompletionException && failure.getCause() != null
                ? failure.getCause() : failure;
        String replIdText = DeferredHandle.replIdText(replId);
        if (cause instanceof IllegalStateException || cause instanceof IllegalArgumentException) {
            // Programming error such as "close is not allowed"; retrying cannot succeed.
            logger.error("[closeHandle][{}][not-deferred] repl_id={} {}", reason, replIdText, operation, cause);
            return;
        }
        DeferredHandleCloser closer = DeferredHandleCloser.of(fs);
        if (closer == null) {
            logger.error("[closeHandle][{}][skip] no deferred closer, repl_id={} {}", reason, replIdText,
                    operation, cause);
            return;
        }
        // defer() logs the entry with repl_id, fileKey and the cause type / message, and logs ERROR itself
        // when it rejects after shutdown.
        if (closer.defer(file, replId, operation, cause)) {
            logCase(CASE_CLOSE_DEFERRED);
            logger.warn("[closeHandle][{}][deferred] repl_id={} {}", reason, replIdText, operation);
        }
    }

    private static void deferOnLateFailure(AsyncFileSystem fs, AbstractStorageFile file, ReplId replId,
                                           CompletableFuture<Void> closeFuture, String operation) {
        closeFuture.whenComplete((ignored, err) -> {
            if (err != null) {
                closeFailed(fs, file, replId, operation, "late-fail", CASE_CLOSE_FAIL, err);
            }
        });
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

    private static boolean awaitCloseFuture(AsyncFileSystem fs, AbstractStorageFile file, ReplId replId,
                                            CompletableFuture<Void> closeFuture, String operation,
                                            boolean allowRejectedRetry) {
        try {
            closeFuture.get(currentTimeoutMillis(), TimeUnit.MILLISECONDS);
            return true;
        } catch (TimeoutException e) {
            logCase(CASE_CLOSE_TIMEOUT);
            logger.warn("[closeHandle][timeout] {}", operation);
            // Do not cancel: TailCache may still release the write slot when the task runs.
            deferOnLateFailure(fs, file, replId, closeFuture, operation);
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logCase(CASE_INTERRUPTED);
            logger.warn("[closeHandle][interrupted] {}", operation);
            deferOnLateFailure(fs, file, replId, closeFuture, operation);
            return false;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (allowRejectedRetry && isRejectedExecution(cause)) {
                logCase(CASE_CLOSE_REJECTED);
                logger.warn("[closeHandle][rejected][retry] {}", operation, cause);
                CompletableFuture<Void> retryFuture = invokeCloseWithOneRetry(fs, file, replId, operation);
                // Second await: no further RejectedExecution retry (avoid loops).
                return retryFuture != null && awaitCloseFuture(fs, file, replId, retryFuture, operation, false);
            }
            closeFailed(fs, file, replId, operation, "fail", CASE_CLOSE_FAIL, cause);
            return false;
        } catch (Throwable t) {
            // e.g. CancellationException: the future is done, so no close of the file is running.
            closeFailed(fs, file, replId, operation, "unexpected", CASE_CLOSE_FAIL, t);
            return false;
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

    /**
     * Write {@code data} in one shot. Recreates the ByteBuf per attempt because TailCache {@code release}s
     * on sync failure. Same D7 retry boundary as {@code writeAndAwait}: sync {@link Exception}
     * before a future is returned is retried once; await failure after a future exists is not.
     */
    public static void writeAllBytes(AsyncFileSystem fs, AsyncFile file, byte[] data, String operation)
            throws IOException {
        for (int attempt = 0; attempt < WRITE_SYNC_THROW_MAX_ATTEMPTS; attempt++) {
            ByteBuf buf = Unpooled.wrappedBuffer(data);
            buf.retain();
            CompletableFuture<Long> future;
            try {
                future = fs.write(file, buf);
            } catch (Exception e) {
                buf.release();
                if (attempt + 1 < WRITE_SYNC_THROW_MAX_ATTEMPTS) {
                    logWriteSyncRetry("writeAllBytes", operation, e);
                    continue;
                }
                logWriteSyncTerminal(e);
                throw rethrowWriteFailure(e);
            } catch (Throwable t) {
                buf.release();
                logFsException(t);
                throw rethrowWriteThrowable(t, operation);
            }
            try {
                long written = awaitFuture(future, operation);
                if (written != data.length) {
                    logCase(CASE_SHORT_WRITE);
                    throw new IOException("short async write, expected " + data.length + " but wrote " + written
                            + ": " + operation);
                }
                return;
            } catch (Throwable t) {
                throw rethrowWriteThrowable(t, operation);
            } finally {
                buf.release();
            }
        }
        throw new IllegalStateException("unreachable writeAllBytes retry loop: " + operation);
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
     * Write with one retry on synchronous {@link Exception} from {@code write} (before a future exists).
     * D7: sync throw = not written. After a future is returned, await failure (timeout / interrupt /
     * future exception) is not retried — the file path may already have a partial write.
     * Extra {@code retain} so TailCache {@code release} on sync fail leaves the buffer usable for retry.
     * First sync throw does not {@code release} here (FS fail path already did). Terminal failure
     * (second sync throw / await fail) drops the guard retain.
     */
    private static long writeAndAwaitInternal(ByteBuf data, int expectedLength, String operation,
                                              FsCallFromBuf writeCall) throws IOException {
        for (int attempt = 0; attempt < WRITE_SYNC_THROW_MAX_ATTEMPTS; attempt++) {
            data.retain();
            CompletableFuture<Long> future;
            try {
                future = writeCall.call(data);
            } catch (Exception e) {
                if (attempt + 1 < WRITE_SYNC_THROW_MAX_ATTEMPTS) {
                    logWriteSyncRetry("writeAndAwait", operation, e);
                    continue;
                }
                data.release();
                logWriteSyncTerminal(e);
                throw rethrowWriteFailure(e);
            } catch (Throwable t) {
                data.release();
                logFsException(t);
                throw rethrowWriteThrowable(t, operation);
            }
            try {
                long flushed = awaitFuture(future, operation);
                if (flushed != expectedLength) {
                    logCase(CASE_SHORT_WRITE);
                    throw new IOException("short async write, expected " + expectedLength + " but flushed " + flushed
                            + ": " + operation);
                }
                data.release();
                return flushed;
            } catch (Throwable t) {
                data.release();
                throw rethrowWriteThrowable(t, operation);
            }
        }
        throw new IllegalStateException("unreachable writeAndAwait retry loop: " + operation);
    }

    @FunctionalInterface
    private interface FsCallFromBuf {
        CompletableFuture<Long> call(ByteBuf data) throws Exception;
    }

    private static void logWriteSyncRetry(String method, String operation, Exception e) {
        if (e instanceof OperationNotExecutedException) {
            logger.warn("[{}][not-executed][retry] {}", method, operation, e);
            logCase(CASE_NOT_EXECUTED_RETRY);
            return;
        }
        logger.warn("[{}][sync-fail][retry] {}", method, operation, e);
        logFsException(e);
    }

    private static void logWriteSyncTerminal(Exception e) {
        logFsException(e);
    }

    /**
     * Map an FS/IO failure to a fixed EventMonitor case. Matches the thrown type only
     * ({@code TailCacheFileSystem} throws ONE / {@link CacheMemoryReserveException} as the outer type).
     */
    static String classifyFailure(Throwable t) {
        if (t instanceof OperationNotExecutedException) {
            return CASE_NOT_EXECUTED;
        }
        if (t instanceof CacheMemoryReserveException) {
            return CASE_MEMORY_RESERVE_FAIL;
        }
        return CASE_UNKNOWN_FAIL;
    }

    private static IOException rethrowWriteFailure(Exception e) throws IOException {
        if (e instanceof IOException) {
            throw (IOException) e;
        }
        if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        }
        throw new IOException(e);
    }

    private static IOException rethrowWriteThrowable(Throwable t, String operation) throws IOException {
        if (t instanceof Error) {
            throw (Error) t;
        }
        if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        }
        if (t instanceof IOException) {
            throw (IOException) t;
        }
        throw new IOException("unexpected async write failure: " + operation, t);
    }

    private static void logFsException(Throwable t) {
        logCase(classifyFailure(t));
    }

    private static void logCase(String caseName) {
        EventMonitor.DEFAULT.logEvent(EVENT_TYPE, caseName);
    }
}

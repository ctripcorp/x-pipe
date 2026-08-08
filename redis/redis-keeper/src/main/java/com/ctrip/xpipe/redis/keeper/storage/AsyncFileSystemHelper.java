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

    public static <T> T await(CompletableFuture<T> future, String operation) throws IOException {
        return await(future, operation, currentTimeoutMillis(), TimeUnit.MILLISECONDS);
    }

    public static <T> T await(CompletableFuture<T> future, String operation, long timeout, TimeUnit unit)
            throws IOException {
        return doAwait(future, operation, timeout, unit, () -> future.cancel(false));
    }

    /**
     * Await an {@code open} future. On timeout, abandon: a late successful open is best-effort
     * {@code fs.close}'d so the handle is never handed to the caller (avoids sticky
     * {@code writer already open}). Timeout still surfaces as {@link IOException}.
     * <p>
     * Does <b>not</b> {@code cancel} the open future: {@link CompletableFuture#cancel} on a
     * {@code supplyAsync} future discards the result while the IO task may still finish and hold
     * the write slot — then abandon cannot reclaim the handle.
     * <p>
     * Timeout budget follows {@link #await} / {@link #runWithIoTimeout}.
     */
    public static <T extends AbstractStorageFile> T awaitOpen(AsyncFileSystem fs, CompletableFuture<T> future,
                                                              String operation) throws IOException {
        return doAwait(future, operation, currentTimeoutMillis(), TimeUnit.MILLISECONDS,
                () -> abandonOpen(fs, future, operation));
    }

    private static long currentTimeoutMillis() {
        Long override = IO_TIMEOUT_MILLIS.get();
        return override != null ? override : DEFAULT_IO_TIMEOUT_MILLIS;
    }

    @FunctionalInterface
    private interface OnTimeout {
        void accept();
    }

    private static <T> T doAwait(CompletableFuture<T> future, String operation, long timeout, TimeUnit unit,
                                 OnTimeout onTimeout) throws IOException {
        try {
            return future.get(timeout, unit);
        } catch (InterruptedException e) {
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
        future.whenComplete((file, err) -> {
            if (err != null) {
                logger.info("[awaitOpen][abandon][late-fail] {}", operation, err);
                return;
            }
            if (file == null) {
                return;
            }
            closeAbandoned(fs, file, operation);
        });
    }

    private static void closeAbandoned(AsyncFileSystem fs, AbstractStorageFile file, String operation) {
        CompletableFuture<Void> closeFuture;
        try {
            if (file instanceof AsyncFile) {
                closeFuture = fs.close((AsyncFile) file);
            } else if (file instanceof AsyncSegmentFile) {
                closeFuture = fs.close((AsyncSegmentFile) file);
            } else {
                logCase(CASE_ABANDON_CLOSE_FAIL);
                logger.error("[awaitOpen][abandon][unknown-type] {} {}", operation, file.getClass().getName());
                return;
            }
        } catch (Throwable t) {
            logCase(CASE_ABANDON_CLOSE_FAIL);
            logger.error("[awaitOpen][abandon][close-fail] {}", operation, t);
            return;
        }
        closeFuture.whenComplete((ignored, closeErr) -> {
            if (closeErr != null) {
                logCase(CASE_ABANDON_CLOSE_FAIL);
                logger.error("[awaitOpen][abandon][close-fail] {}", operation, closeErr);
            } else {
                logCase(CASE_ABANDON_CLOSE);
                logger.info("[awaitOpen][abandon][closed] {}", operation);
            }
        });
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
        ByteBuf buf = Unpooled.wrappedBuffer(data);
        buf.retain();
        try {
            CompletableFuture<Long> future = fs.write(file, buf);
            long written = await(future, operation);
            if (written != data.length) {
                logCase(CASE_SHORT_WRITE);
                throw new IOException("short async write, expected " + data.length + " but wrote " + written
                        + ": " + operation);
            }
        } finally {
            buf.release();
        }
    }

    public static byte[] readAllBytes(AsyncFileSystem fs, AsyncFile file, long size, long offset, String operation)
            throws IOException {
        ByteBuf buf = await(fs.read(file, size, offset), operation);
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
        CompletableFuture<Long> future = fs.write(file, data);
        long flushed = await(future, operation);
        if (flushed != expectedLength) {
            logCase(CASE_SHORT_WRITE);
            throw new IOException("short async write, expected " + expectedLength + " but flushed " + flushed
                    + ": " + operation);
        }
        return flushed;
    }

    public static long writeAndAwait(AsyncFileSystem fs, AsyncFile file, ByteBuf data, int expectedLength,
                                     String operation) throws IOException {
        CompletableFuture<Long> future = fs.write(file, data);
        long flushed = await(future, operation);
        if (flushed != expectedLength) {
            logCase(CASE_SHORT_WRITE);
            throw new IOException("short async write, expected " + expectedLength + " but flushed " + flushed
                    + ": " + operation);
        }
        return flushed;
    }

    private static void logFsException(Throwable t) {
        EventMonitor.DEFAULT.logEvent(EVENT_TYPE, t.getClass().getSimpleName());
    }

    private static void logCase(String caseName) {
        EventMonitor.DEFAULT.logEvent(EVENT_TYPE, caseName);
    }
}

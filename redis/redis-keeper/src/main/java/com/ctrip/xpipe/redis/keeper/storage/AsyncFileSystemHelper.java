package com.ctrip.xpipe.redis.keeper.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class AsyncFileSystemHelper {

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
        Long override = IO_TIMEOUT_MILLIS.get();
        long timeoutMillis = override != null ? override : DEFAULT_IO_TIMEOUT_MILLIS;
        return await(future, operation, timeoutMillis, TimeUnit.MILLISECONDS);
    }

    public static <T> T await(CompletableFuture<T> future, String operation, long timeout, TimeUnit unit)
            throws IOException {
        try {
            return future.get(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while waiting async file IO: " + operation, e);
        } catch (TimeoutException e) {
            throw new IOException("timeout waiting async file IO: " + operation, e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof StorageIOException && cause.getCause() instanceof IOException) {
                throw (IOException) cause.getCause();
            }
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException("async file IO failed: " + operation, cause);
        }
    }

    public static void writeAllBytes(AsyncFileSystem fs, AsyncFile file, byte[] data, String operation)
            throws IOException {
        ByteBuf buf = Unpooled.wrappedBuffer(data);
        buf.retain();
        try {
            CompletableFuture<Long> future = fs.write(file, buf);
            long written = await(future, operation);
            if (written != data.length) {
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
            throw new IOException("short async write, expected " + expectedLength + " but flushed " + flushed
                    + ": " + operation);
        }
        return flushed;
    }
}

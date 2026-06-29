package com.ctrip.xpipe.redis.keeper.storage;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class AsyncFileSystemHelper {

    private static final long DEFAULT_IO_TIMEOUT_SECONDS = 1;

    private AsyncFileSystemHelper() {
    }

    public static <T> T await(CompletableFuture<T> future, String operation) throws IOException {
        try {
            return future.get(DEFAULT_IO_TIMEOUT_SECONDS, TimeUnit.SECONDS);
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

    public static byte[] readFully(AsyncFileSystem asyncFileSystem, AsyncFile asyncFile, long size, String path)
            throws IOException {
        if (size > Integer.MAX_VALUE) {
            throw new IOException("async file too large: " + path);
        }

        byte[] data = new byte[(int) size];
        int offset = 0;
        while (offset < data.length) {
            int length = data.length - offset;
            byte[] chunk = new byte[length];
            int read = await(asyncFileSystem.read(asyncFile, length, offset, chunk), "read " + path);
            if (read <= 0) {
                throw new IOException("failed to read full async file: " + path);
            }
            System.arraycopy(chunk, 0, data, offset, read);
            offset += read;
        }
        return data;
    }
}

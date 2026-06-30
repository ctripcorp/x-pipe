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
}

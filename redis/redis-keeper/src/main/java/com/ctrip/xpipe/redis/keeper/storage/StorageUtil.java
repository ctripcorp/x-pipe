package com.ctrip.xpipe.redis.keeper.storage;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

class StorageUtil {

    static <T> CompletableFuture<T> supply(ExecutorService executor, java.util.function.Supplier<T> task) {
        try {
            return CompletableFuture.supplyAsync(task, executor);
        } catch (RejectedExecutionException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    static CompletableFuture<Void> run(ExecutorService executor, Runnable task) {
        try {
            return CompletableFuture.runAsync(task, executor);
        } catch (RejectedExecutionException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    static String fileKey(String path) {
        return path;
    }

    static String segmentKey(String path, String prefix) {
        return path + "\0" + prefix;
    }

    // Translates a checked IOException into a runtime exception that reflects
    // recovery semantics: StaleStateException for mismatched state, StorageIOException for
    // genuine transient IO failures, IllegalArgumentException for invalid arguments.
    static RuntimeException wrapIOException(IOException e) {
        if (e instanceof NoSuchFileException
                || e instanceof FileAlreadyExistsException
                || e instanceof DirectoryNotEmptyException
                || e instanceof ClosedChannelException) {
            return new StaleStateException(e);
        }
        if (e instanceof NotDirectoryException) {
            return new IllegalArgumentException(e);
        }
        String msg = e.getMessage();
        if (msg != null && msg.startsWith("Input/output error")) {
            return new EIOException(e);
        }
        return new StorageIOException(e);
    }
}

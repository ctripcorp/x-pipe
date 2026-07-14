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

    static void requireOpen(AbstractStorageFile file) {
        if (file.closed) {
            throw new IllegalStateException("file is closed: " + file.identifier());
        }
    }

    static void requireCacheOpen(AbstractStorageFile file) {
        if (file.cacheClosed) {
            throw new IllegalStateException("file cache is closed: " + file.identifier());
        }
    }

    static void requireWriteMode(AbstractStorageFile file) {
        if (!file.canWrite()) {
            throw new IllegalArgumentException("operation requires write mode: " + file.identifier());
        }
    }

    // Translates a checked IOException into a runtime exception that reflects
    // recovery semantics:
    //   StaleStateException  - mismatched state (file not found, already exists, channel closed, etc.)
    //   SocketErrorException - socket-level errors (broken pipe, connection reset, closed channel on target)
    //   EIOException         - Input/output error (EIO); on some filesystems like TFS this is transient and recoverable
    //   StorageIOException   - other transient IO failures
    //   IllegalArgumentException - invalid arguments (e.g. path is not a directory)
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
        if (msg != null) {
            if (msg.contains("Broken pipe") || msg.contains("Connection reset")) {
                return new SocketErrorException(e);
            }
            if (msg.startsWith("Input/output error")) {
                return new EIOException(e);
            }
        }
        return new StorageIOException(e);
    }
}

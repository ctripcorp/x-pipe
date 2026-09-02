package com.ctrip.xpipe.redis.keeper.storage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StorageUtil {

    private static final Logger logger = LoggerFactory.getLogger(StorageUtil.class);

    static boolean existsSync(Path p) throws IOException {
        // use checkAccess rather than Files.exists because we want to throw IOException if happens.
        try {
            p.getFileSystem().provider().checkAccess(p);
            return true;
        } catch (NoSuchFileException e) {
            return false;
        }
    }

    static List<String> listNamesSync(Path dir) throws IOException {
        // use Files.list rather than File.list because we want to throw IOException if happens.
        try (Stream<Path> entries = Files.list(dir)) {
            return entries.map(p -> p.getFileName().toString()).collect(Collectors.toList());
        } catch (UncheckedIOException e) {
            // Files.list only throws IOException while opening the directory. Failures during
            // iteration (readdir) and on stream close surface as UncheckedIOException,
            // can get the real IOException from the cause.
            throw e.getCause();
        }
    }

    // use readAttributes rather than Files.isDirectory / Files.isRegularFile because we want to
    // throw IOException if happens. NoSuchFileException is thrown too rather than reported as
    // false: "the path is gone" is not an answer to "is it a directory", and folding it into
    // false hides concurrent deletion from the caller. Callers that genuinely want a missing
    // path to read as false catch NoSuchFileException themselves.
    static boolean isDirectorySync(Path p) throws IOException {
        return Files.readAttributes(p, BasicFileAttributes.class).isDirectory();
    }

    static boolean isRegularFileSync(Path p) throws IOException {
        return Files.readAttributes(p, BasicFileAttributes.class).isRegularFile();
    }

    static <T> T awaitFuture(CompletableFuture<T> future, String path, long timeoutMs, boolean throwOnFailure) {
        try {
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException | InterruptedException e) {
            throw new OperationNotExecutedException(path, e);
        } catch (ExecutionException e) {
            if (throwOnFailure) {
                throw new OperationNotExecutedException(path, e);
            }
            logger.warn("prior IO failed for {}, ignoring during wait", path, e);
            return null;
        }
    }

    // clean disposes of a result the abandoned await will never receive; it runs only when the
    // await gave up but the task later succeeded. Pass null when the result needs no disposal
    static <T> T awaitIoCachePrep(ExecutorService executor, AbstractStorageFile file, String registerKey,
            long timeoutMs, BiConsumer<String, CompletableFuture<?>> register,
            Supplier<T> task, Consumer<T> clean) {
        CompletableFuture<T> future = supply(executor, () -> {
            requireOpen(file);
            return task.get();
        });
        if (registerKey != null) {
            register.accept(registerKey, future);
        }
        try {
            return awaitFuture(future, file.path, timeoutMs, true);
        } catch (Exception e) {
            if (clean != null) {
                future.whenComplete((value, error) -> {
                    if (error == null) {
                        try {
                            clean.accept(value);
                        } catch (Throwable cleanError) {
                            logger.warn("clean abandoned IO cache prep result failed for {}", file.path, cleanError);
                        }
                    }
                });
            }
            throw e;
        }
    }

    // Close detached channels, logging failures. Never throws.
    static void closeChannels(List<FileChannel> channels) {
        if (channels == null || channels.isEmpty()) {
            return;
        }
        for (FileChannel ch : channels) {
            if (ch == null) {
                continue;
            }
            try {
                ch.close();
            } catch (IOException e) {
                logger.error("failed to close channel", e);
            }
        }
    }

    static <T> CompletableFuture<T> supply(ExecutorService executor, java.util.function.Supplier<T> task) {
        try {
            return CompletableFuture.supplyAsync(task, executor);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    static <T> CompletableFuture<T> supply(ExecutorService executor, java.util.function.Supplier<T> task, ByteBuf data) {
        try {
            return CompletableFuture.supplyAsync(task, executor);
        } catch (Exception e) {
            data.release();
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

    static String asyncFileKey(String path) {
        return "f\0" + path;
    }

    static String segmentKey(String path, String prefix) {
        return "s\0" + path + "\0" + prefix;
    }

    static long chunkCapacityForBytes(long bytes, long chunkSize) {
        if (bytes == 0) {
            return chunkSize;
        }
        return (bytes + chunkSize - 1) / chunkSize * chunkSize;
    }

    static void requireOpen(AbstractStorageFile file) {
        if (file.closed) {
            throw new IllegalStateException("file is closed: " + file.path);
        }
    }

    static void requireWriteMode(AbstractStorageFile file) {
        if (!file.canWrite()) {
            throw new IllegalArgumentException("operation requires write mode: " + file.path);
        }
    }

    // Translates a checked IOException into a runtime exception that reflects recovery semantics:
    //   StaleStateException      - mismatched filesystem state; plain retry cannot repair it
    //   SocketErrorException     - socket-level errors (broken pipe, reset, closed transfer target)
    //   EIOException             - Input/output error (EIO); the current file channel must be replaced
    //   StorageIOException       - other IO failures, including closed persistent channels and ENOSPC
    //   IllegalArgumentException - invalid arguments (e.g. path is not a directory)
    static RuntimeException wrapIOException(IOException e) {
        return wrapIOException(e, null);
    }

    static boolean isNoSpace(IOException e) {
        String message = e.getMessage();
        return message != null && message.contains("No space left on device");
    }

    // target is supplied only by transferTo: ClosedChannelException does not identify which side
    // closed, so distinguish a closed socket target from a closed persistent file channel here.
    static RuntimeException wrapIOException(IOException e, WritableByteChannel target) {
        if (e instanceof ClosedChannelException) {
            if (target != null && !target.isOpen()) {
                return new SocketErrorException(e);
            }
            return new StorageIOException(e);
        }
        if (e instanceof NoSuchFileException
                || e instanceof FileAlreadyExistsException
                || e instanceof DirectoryNotEmptyException) {
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

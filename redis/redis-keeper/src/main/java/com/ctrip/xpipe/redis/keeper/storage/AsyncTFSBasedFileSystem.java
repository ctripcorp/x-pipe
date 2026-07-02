package com.ctrip.xpipe.redis.keeper.storage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// low level file system implementation, not used directly.
// TFS guarantees that metadata is synchronously delivered: create/rm operations are durable after returning.
// TFS guarantees that close will flush all data before returning.
public class AsyncTFSBasedFileSystem implements AsyncFileSystem {

    private static final Logger logger = LoggerFactory.getLogger(AsyncTFSBasedFileSystem.class);
    private static final String TMP_REP_ = "TMP_REP_";
    private static final int LOCK_STRIPES = 32;

    private final ExecutorService ioExecutor;

    // Registry of shared segment-dir state, keyed by "dirPath\0prefix".
    // First opener wins on DirEntry construction.
    private final ConcurrentHashMap<String, DirEntry> registry = new ConcurrentHashMap<>();

    private final Object[] openCloseLocks = new Object[LOCK_STRIPES];

    public AsyncTFSBasedFileSystem(int threadCount) {
        this.ioExecutor = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < LOCK_STRIPES; i++) openCloseLocks[i] = new Object();
    }

    @Override
    public void shutdown() {
        ioExecutor.shutdown();
    }

    private Object lockFor(String key) {
        return openCloseLocks[(key.hashCode() & 0x7fffffff) % LOCK_STRIPES];
    }

    private static String registryKey(String path, String prefix) {
        return path + "\0" + prefix;
    }

    // Translates a checked IOException into a runtime exception that reflects
    // recovery semantics: StaleStateException for mismatched state, StorageIOException for
    // genuine transient IO failures.IllegalArgumentExceptions for invalid arguments.
    private static RuntimeException wrap(IOException e) {
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

    // ---- AsyncFile ----

    // atomicReplace uses tmp file approach instead of rename because tfs currently does not support rename.
    // Tmp file format: [8-byte length][data].
    @Override
    public CompletableFuture<AsyncFile> open(String path, boolean write, boolean atomicReplace, boolean lenient) {
        if (atomicReplace && !write) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("atomicReplace requires write=true"));
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                Path p = Paths.get(path);
                if (lenient && Files.exists(p) && !Files.isRegularFile(p)) {
                    return new AsyncFile(path, null, atomicReplace, write);
                }
                if (atomicReplace) {
                    recoverFromTmp(p);
                }
                FileChannel ch;
                if (write) {
                    ch = FileChannel.open(p, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
                    if (!atomicReplace) ch.position(ch.size());
                } else {
                    ch = FileChannel.open(p, StandardOpenOption.READ);
                }
                return new AsyncFile(path, ch, atomicReplace, write);
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Boolean> isFile(AsyncFile file) {
        return CompletableFuture.supplyAsync(
                () -> Files.isRegularFile(Paths.get(file.path)), ioExecutor);
    }

    @Override
    public CompletableFuture<Boolean> isDirectory(String path) {
        return CompletableFuture.supplyAsync(
                () -> Files.isDirectory(Paths.get(path)), ioExecutor);
    }

    @Override
    public CompletableFuture<Long> lastModified(AsyncFile file) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return Files.getLastModifiedTime(Paths.get(file.path)).toMillis();
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> position(AsyncFile file, long position) {
        if (file.writeMode) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("position() requires read mode"));
        }
        return CompletableFuture.runAsync(() -> {
            try {
                file.channel.position(position);
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Long> read(AsyncFile file, long length, long offset, byte[] buffer) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return readFully(file.channel, length, offset, buffer);
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Long> read(AsyncFile file, long length, byte[] buffer) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return readFully(file.channel, length, buffer);
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Long> write(AsyncFile file, byte[] data, long length) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                if (file.atomicReplace) {
                    return atomicReplaceWrite(file, data, Math.min(length, data.length));
                }
                return writeFully(file.channel, data, length);
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> delete(String path) {
        return CompletableFuture.runAsync(() -> {
            try {
                Files.deleteIfExists(Paths.get(path));
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Boolean> exists(String path) {
        return CompletableFuture.supplyAsync(
                () -> Files.exists(Paths.get(path)), ioExecutor);
    }

    @Override
    public CompletableFuture<Long> size(AsyncFile file) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return file.channel.size();
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Boolean> mkdir(String path, boolean recursive) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                if (recursive) {
                    Files.createDirectories(Paths.get(path));
                } else {
                    try {
                        Files.createDirectory(Paths.get(path));
                    } catch (FileAlreadyExistsException e) {
                        if (!Files.isDirectory(Paths.get(path))) {
                            throw e;
                        }
                    }
                }
                return true;
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Boolean> rmdir(String path, boolean recursive) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Path dir = Paths.get(path);
                if (!Files.exists(dir)) return true;
                if (!Files.isDirectory(dir)) throw new IllegalArgumentException("not a directory: " + path);
                if (recursive) {
                    Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                            Files.delete(file);
                            return FileVisitResult.CONTINUE;
                        }
                        @Override
                        public FileVisitResult postVisitDirectory(Path d, IOException exc) throws IOException {
                            if (exc != null) throw exc;
                            Files.delete(d);
                            return FileVisitResult.CONTINUE;
                        }
                    });
                } else {
                    Files.delete(dir);
                }
                return true;
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Boolean> truncate(AsyncFile file, long size) {
        if (!file.writeMode) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("truncate() requires write mode"));
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                file.channel.truncate(size);
                if (!file.atomicReplace) {
                    file.channel.position(size);
                }
                return true;
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> close(AsyncFile file) {
        return CompletableFuture.runAsync(() -> {
            try {
                file.channel.close();
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> fsync(AsyncFile file) {
        if (!file.writeMode) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("fsync() requires write mode"));
        }
        return CompletableFuture.runAsync(() -> {
            try {
                file.channel.force(true);
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<List<String>> list(String path) {
        return CompletableFuture.supplyAsync(() -> {
            String[] names = new File(path).list();
            if (names == null) return Collections.emptyList();
            List<String> filtered = new ArrayList<>(names.length);
            for (String name : names) {
                if (!name.startsWith(TMP_REP_)) filtered.add(name);
            }
            return filtered;
        }, ioExecutor);
    }

    // ---- AsyncFile helpers ----

    private long readFully(FileChannel ch, long length, byte[] buffer) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(buffer, 0, (int) Math.min(length, buffer.length));
        long totalRead = 0;
        while (buf.hasRemaining()) {
            int n = ch.read(buf);
            if (n < 0) break;
            totalRead += n;
        }
        return totalRead;
    }

    private long readFully(FileChannel ch, long length, long offset, byte[] buffer) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(buffer, 0, (int) Math.min(length, buffer.length));
        long totalRead = 0;
        while (buf.hasRemaining()) {
            int n = ch.read(buf, offset + totalRead);
            if (n < 0) break;
            totalRead += n;
        }
        return totalRead;
    }

    private long writeFully(FileChannel ch, byte[] data, long length) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(data, 0, (int) Math.min(length, data.length));
        long totalWritten = 0;
        while (buf.hasRemaining()) {
            totalWritten += ch.write(buf);
        }
        return totalWritten;
    }

    private Path getTmpPath(String filePath) {
        Path p = Paths.get(filePath);
        return Paths.get(p.getParent().toString(), TMP_REP_ + p.getFileName());
    }

    private void recoverFromTmp(Path filePath) throws IOException {
        Path tmpPath = getTmpPath(filePath.toString());
        if (!Files.exists(tmpPath)) {
            return;
        }
        try (FileChannel tmpCh = FileChannel.open(tmpPath, StandardOpenOption.READ);
             FileChannel fileCh = FileChannel.open(filePath, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            long tmpSize = tmpCh.size();
            if (tmpSize < 8) {
                logger.warn("tmp file size too small: {} < 8, deleting {}", tmpSize, tmpPath);
                Files.deleteIfExists(tmpPath);
                return;
            }
            byte[] lenBytes = new byte[8];
            long lenRead = readFully(tmpCh, 8, lenBytes);
            if (lenRead != 8) {
                logger.warn("failed to read length from tmp file: read {} bytes, expected 8, deleting {}", lenRead, tmpPath);
                Files.deleteIfExists(tmpPath);
                return;
            }
            ByteBuffer lenBuf = ByteBuffer.wrap(lenBytes);
            long expectedLen = lenBuf.getLong();
            long expectedTmpSize = 8 + expectedLen;
            if (tmpSize != expectedTmpSize) {
                logger.warn("tmp file size mismatch: actual {} != expected {}, deleting {}", tmpSize, expectedTmpSize, tmpPath);
                Files.deleteIfExists(tmpPath);
                return;
            }
            byte[] dataBytes = new byte[(int) expectedLen];
            long dataRead = readFully(tmpCh, expectedLen, dataBytes);
            if (dataRead != expectedLen) {
                logger.error("failed to read data from tmp file: read {} bytes, expected {}, deleting {}. This should not happen.",
                    dataRead, expectedLen, tmpPath);
                Files.deleteIfExists(tmpPath);
                return;
            }
            fileCh.truncate(0);
            writeFully(fileCh, dataBytes, dataBytes.length);
            fileCh.force(true);
            Files.deleteIfExists(tmpPath);
            logger.info("recovered from tmp file: {}", tmpPath);
        }
    }

    private long atomicReplaceWrite(AsyncFile file, byte[] data, long length) throws IOException {
        Path tmpPath = getTmpPath(file.path);
        try (FileChannel tmpCh = FileChannel.open(tmpPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            byte[] lenBytes = ByteBuffer.allocate(8).putLong(length).array();
            writeFully(tmpCh, lenBytes, 8);
            writeFully(tmpCh, data, length);
            tmpCh.force(true);
        }
        file.channel.truncate(0);
        file.channel.position(0);
        long written = writeFully(file.channel, data, length);
        file.channel.force(true);
        Files.deleteIfExists(tmpPath);
        return written;
    }

    // ---- AsyncSegmentFile ----

    @Override
    public CompletableFuture<AsyncSegmentFile> open(String path, String prefix,
            List<String> indexPrefixes, boolean write) {
        return CompletableFuture.supplyAsync(() -> {
            String key = registryKey(path, prefix);
            DirEntry entry;
            boolean iAmInitializer = false;

            synchronized (lockFor(key)) {
                entry = registry.get(key);
                if (entry == null) {
                    entry = new DirEntry();
                    registry.put(key, entry);
                    iAmInitializer = true;
                } else if (write && entry.writerOpen) {
                    throw new IllegalStateException("writer already open for " + key);
                }
                if (write) entry.writerOpen = true;
                entry.refCount++;
            }

            if (iAmInitializer) {
                try {
                    initFromDisk(entry, path, prefix, indexPrefixes);
                } catch (Throwable t) {
                    logger.error("Failed to init segment file {}{} indexPrefixes:{}", path, prefix, indexPrefixes, t);
                    entry.initFailed = true;
                } finally {
                    entry.initDone.countDown();
                }
            } else {
                try {
                    entry.initDone.await();
                } catch (InterruptedException e) {
                    releaseDirEntry(key, write);
                    throw new RuntimeException(e);
                }
            }
            if (entry.initFailed) {
                releaseDirEntry(key, entry, write);
                throw new StorageIOException("init failed for " + key);
            }

            AsyncSegmentFile file = new AsyncSegmentFile(path, prefix, indexPrefixes, key, write);
            try {
                file.openInitialResources(entry.state);
            } catch (IOException e) {
                releaseDirEntry(key, entry, write);
                throw wrap(e);
            } catch (Throwable t) {
                // make sure to release the entry even if error like NPE is thrown
                releaseDirEntry(key, entry, write);
                throw t;
            }
            return file;
        }, ioExecutor);
    }

    private void initFromDisk(DirEntry entry, String path, String prefix, List<String> indexPrefixes) throws IOException {
        Path dir = Paths.get(path);
        String[] names = new File(path).list();
        if (names == null) {
            if (!Files.exists(dir)) {
                throw new IllegalArgumentException("directory does not exist: " + path);
            }
            if (!Files.isDirectory(dir)) {
                throw new IllegalArgumentException("not a directory: " + path);
            }
            throw new IOException("failed to list directory: " + path);
        }
        AsyncSegmentFile.initFromFiles(entry, path, prefix, indexPrefixes, Arrays.asList(names));
    }

    private void releaseDirEntry(String key, boolean write) {
        synchronized (lockFor(key)) {
            DirEntry entry = registry.get(key);
            if (entry == null) return;
            if (write) entry.writerOpen = false;
            if (--entry.refCount == 0) registry.remove(key);
        }
    }

    private DirEntry entryOrThrow(AsyncSegmentFile file) {
        DirEntry entry = registry.get(file.key);
        if (entry == null) throw new IllegalStateException("file is closed: " + file.key);
        return entry;
    }

    @Override
    public CompletableFuture<Void> position(AsyncSegmentFile file, long offset) {
        if (file.writeMode) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("position() is not supported in write mode"));
        }
        return CompletableFuture.runAsync(() -> {
            try {
                file.readPosition = offset;
                SegmentDirState s = entryOrThrow(file).state;
                if (file.switchToSegment(offset, s)) {
                    file.currentSegmentChannel.position(offset - file.openedSegmentStartOffset);
                }
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Long> read(AsyncSegmentFile file, long length, byte[] buffer) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                SegmentDirState s = entryOrThrow(file).state;
                if (file.currentSegmentChannel == null) {
                    if (!file.switchToSegment(file.readPosition, s)) return 0L;
                    file.currentSegmentChannel.position(file.readPosition - file.openedSegmentStartOffset);
                }
                long n = readFully(file.currentSegmentChannel, length, buffer);
                file.readPosition += n;
                boolean atSegmentBoundary = file.readPosition >= file.openedSegmentEndOffset;
                boolean staleTailEof = n == 0
                        && file.openedSegmentStartOffset != s.lastOffset
                        && file.currentSegmentChannel.size() <= file.readPosition - file.openedSegmentStartOffset;
                if (atSegmentBoundary || staleTailEof) {
                    file.closeCurrent();
                    if (file.switchToSegment(file.readPosition, s)) {
                        file.currentSegmentChannel.position(file.readPosition - file.openedSegmentStartOffset);
                    }
                }
                return n;
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Long> read(AsyncSegmentFile file, long length, long offset, byte[] buffer) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                SegmentDirState s = entryOrThrow(file).state;
                if (file.currentSegmentChannel == null
                        || offset < file.openedSegmentStartOffset
                        || offset >= file.openedSegmentEndOffset) {
                    if (!file.switchToSegment(offset, s)) return 0L;
                }
                long physicalOffset = offset - file.openedSegmentStartOffset;
                long n = readFully(file.currentSegmentChannel, length, physicalOffset, buffer);
                boolean atSegmentBoundary = offset + n >= file.openedSegmentEndOffset;
                boolean staleTailEof = n == 0
                        && file.openedSegmentStartOffset != s.lastOffset
                        && file.currentSegmentChannel.size() <= physicalOffset;
                if (atSegmentBoundary || staleTailEof) {
                    file.closeCurrent();
                    file.switchToSegment(offset + n, s);
                }
                return n;
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Long> write(AsyncSegmentFile file, byte[] data, long length) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                DirEntry entry = entryOrThrow(file);
                if (entry.state.isEmpty()) {
                    file.openFirstSegmentChannelForWrite(entry);
                }
                return writeFully(file.currentSegmentChannel, data, length);
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Map<String, AsyncFile>> roll(AsyncSegmentFile file) {
        if (!file.writeMode) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("roll() requires write mode"));
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                DirEntry entry = entryOrThrow(file);
                return file.roll(entry);
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public List<Long> list(AsyncSegmentFile file) {
        return entryOrThrow(file).state.offsets();
    }

    @Override
    public long getCurrentSegmentStartOffset(AsyncSegmentFile file) {
        if (file.currentSegmentChannel != null) {
            return file.openedSegmentStartOffset;
        }
        SegmentDirState s = entryOrThrow(file).state;
        if (s.isEmpty()) {
            return 0;
        }
        if (s.contains(file.readPosition)) {
            return file.readPosition;
        }
        return -1;
    }

    @Override
    public CompletableFuture<Map<String, AsyncFile>> getCurrentIndexFiles(AsyncSegmentFile file,
            List<String> indexPrefixes) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                DirEntry entry = entryOrThrow(file);
                SegmentDirState s = entry.state;
                if (s.isEmpty()) {
                    if (file.writeMode) {
                        file.openFirstSegmentChannelForWrite(entry);
                    } else {
                        return new HashMap<String, AsyncFile>();
                    }
                } else if (!file.writeMode) {
                    // Read mode: lazy open the current segment.
                    if (file.currentSegmentChannel == null) {
                        if (!file.switchToSegment(file.readPosition, s)) {
                            return new HashMap<String, AsyncFile>();
                        }
                    }
                }
                return file.getCurrentIndexFiles(indexPrefixes);
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Map<String, AsyncFile>> getCurrentIndexFiles(AsyncSegmentFile file) {
        return getCurrentIndexFiles(file, file.indexPrefixes);
    }

    @Override
    public CompletableFuture<Map<String, AsyncFile>> openIndexFiles(AsyncSegmentFile file, long startOffset) {
        if (file.writeMode) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("openIndexFiles() requires read mode"));
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                Map<String, AsyncFile> result = new HashMap<>();
                if (!entryOrThrow(file).state.contains(startOffset)) return result;
                for (String indexPrefix : file.indexPrefixes) {
                    String fileName = indexPrefix + startOffset;
                    Path p = Paths.get(file.absolutePathOf(fileName));
                    if (!Files.exists(p)) continue;
                    FileChannel ch = FileChannel.open(p, StandardOpenOption.READ);
                    result.put(indexPrefix, new AsyncFile(file.absolutePathOf(fileName), ch, false, false));
                }
                return result;
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Long> size(AsyncSegmentFile file) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                SegmentDirState s = entryOrThrow(file).state;
                if (s.isEmpty()) {
                    return 0L;
                }
                return file.exclusiveEndOffset(s.lastOffset) - s.firstOffset;
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Long> lastModified(AsyncSegmentFile file) {
        SegmentDirState s = entryOrThrow(file).state;
        if (s.isEmpty()) {
            return CompletableFuture.completedFuture(0L);
        }
        return lastModifiedOfSegment(file, s.lastOffset);
    }

    @Override
    public CompletableFuture<Long> lastModifiedOfSegment(AsyncSegmentFile file, long startOffset) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return Files.getLastModifiedTime(file.segmentPath(startOffset)).toMillis();
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Long> sizeOfSegment(AsyncSegmentFile file, long startOffset) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return Files.size(file.segmentPath(startOffset));
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> deleteSegments(AsyncSegmentFile file, List<Long> startOffsets) {
        if (!file.writeMode) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("deleteSegments() requires write mode"));
        }
        return CompletableFuture.runAsync(() -> {
            try {
                DirEntry entry = entryOrThrow(file);
                file.deleteSegments(startOffsets, entry);
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Map<String, AsyncFile>> truncate(AsyncSegmentFile file, long offset) {
        if (!file.writeMode) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("truncate() requires write mode"));
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                DirEntry entry = entryOrThrow(file);
                return file.truncate(offset, entry);
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> close(AsyncSegmentFile file) {
        return CompletableFuture.runAsync(() -> {
            IOException closeErr = null;
            try {
                file.closeCurrent();
            } catch (IOException e) {
                closeErr = e;
            }
            releaseDirEntry(file.key, file.writeMode);
            if (closeErr != null) throw wrap(closeErr);
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> delete(AsyncSegmentFile file) {
        if (!file.writeMode) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("delete() requires write mode"));
        }
        return CompletableFuture.runAsync(() -> {
            try {
                DirEntry entry = entryOrThrow(file);
                file.delete(entry);
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> fsync(AsyncSegmentFile file) {
        if (!file.writeMode) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("fsync() requires write mode"));
        }
        return CompletableFuture.runAsync(() -> {
            try {
                if (file.currentSegmentChannel != null) file.currentSegmentChannel.force(true);
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Long> transferTo(AsyncFile file, long position, long count,
            WritableByteChannel target) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return file.channel.transferTo(position, count, target);
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Long> transferTo(AsyncSegmentFile file, long offset, long count,
            WritableByteChannel target) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                SegmentDirState s = entryOrThrow(file).state;
                if (file.currentSegmentChannel == null
                        || offset < file.openedSegmentStartOffset
                        || offset >= file.openedSegmentEndOffset) {
                    if (!file.switchToSegment(offset, s)) return 0L;
                }
                long physicalOffset = offset - file.openedSegmentStartOffset;
                long n = file.currentSegmentChannel.transferTo(physicalOffset, count, target);
                boolean atSegmentBoundary = offset + n >= file.openedSegmentEndOffset;
                boolean staleTailEof = n == 0
                        && file.openedSegmentStartOffset != s.lastOffset
                        && file.currentSegmentChannel.size() <= physicalOffset;
                if (atSegmentBoundary || staleTailEof) {
                    file.closeCurrent();
                    file.switchToSegment(offset + n, s);
                }
                return n;
            } catch (IOException e) {
                throw wrap(e);
            }
        }, ioExecutor);
    }
}

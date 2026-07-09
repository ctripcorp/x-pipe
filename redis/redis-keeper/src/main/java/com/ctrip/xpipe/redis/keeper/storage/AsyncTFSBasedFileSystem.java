package com.ctrip.xpipe.redis.keeper.storage;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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
    private final long fsyncIntervalBytes;

    // Registry of shared segment-dir state, keyed by "dirPath\0prefix".
    // First opener wins on DirEntry construction.
    private final ConcurrentHashMap<String, DirEntry> dirEntries = new ConcurrentHashMap<>();

    private final Object[] openCloseLocks = new Object[LOCK_STRIPES];

    public AsyncTFSBasedFileSystem(ExecutorService ioExecutor, long fsyncIntervalBytes) {
        this.ioExecutor = ioExecutor;
        this.fsyncIntervalBytes = fsyncIntervalBytes;
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
    // ---- AsyncFile ----

    // atomicReplace uses tmp file approach instead of rename because tfs currently does not support rename.
    // Tmp file format: [8-byte length][data].
    @Override
    public CompletableFuture<AsyncFile> open(String path, boolean write, boolean atomicReplace, boolean lenient, String tenant) {
        if (atomicReplace && !write) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("atomicReplace requires write=true"));
        }
        return StorageUtil.supply(ioExecutor, () -> openSyncInternal(path, write, atomicReplace, lenient));
    }

    @Override
    public AsyncFile openSync(String path, boolean write, boolean atomicReplace, boolean lenient, String tenant) {
        if (atomicReplace && !write) {
            throw new IllegalArgumentException("atomicReplace requires write=true");
        }
        return openSyncInternal(path, write, atomicReplace, lenient);
    }


    private AsyncFile openSyncInternal(String path, boolean write, boolean atomicReplace, boolean lenient) {
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
                try {
                    if (!atomicReplace) ch.position(ch.size());
                } catch (IOException e) {
                    ch.close();
                    throw e;
                }
            } else {
                ch = FileChannel.open(p, StandardOpenOption.READ);
            }
            return new AsyncFile(path, ch, atomicReplace, write);
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
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
        return StorageUtil.supply(ioExecutor, () -> {
            try {
                return Files.getLastModifiedTime(Paths.get(file.path)).toMillis();
            } catch (IOException e) {
                throw StorageUtil.wrapIOException(e);
            }
        });
    }

    @Override
    public CompletableFuture<Void> position(AsyncFile file, long position) {
        if (file.writeMode) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("position() requires read mode"));
        }
        file.position = position;
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<ByteBuf> read(AsyncFile file, long length, long offset) {
        return StorageUtil.supply(ioExecutor, () -> {
            try {
                return readFully(file.channel, length, offset, 0);
            } catch (IOException e) {
                throw StorageUtil.wrapIOException(e);
            }
        });
    }

    @Override
    public ByteBuf readSync(AsyncFile file, long length, long offset, long alignSize) {
        try {
            return readFully(file.channel, length, offset, alignSize);
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }

    @Override
    public CompletableFuture<ByteBuf> read(AsyncFile file, long length) {
        return StorageUtil.supply(ioExecutor, () -> {
            try {
                long pos = file.position;
                ByteBuf buf = readFully(file.channel, length, pos, 0);
                file.position += buf.readableBytes();
                return buf;
            } catch (IOException e) {
                throw StorageUtil.wrapIOException(e);
            }
        });
    }

    @Override
    public CompletableFuture<Long> write(AsyncFile file, ByteBuf data) {
        return StorageUtil.supply(ioExecutor, () -> writeSync(file, data));
    }

    @Override
    public long writeSync(AsyncFile file, ByteBuf data) {
        try {
            if (file.atomicReplace) {
                return atomicReplaceWrite(file, data);
            }
            return writeAndFlush(file, data);
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        } finally {
            data.release();
        }
    }

    @Override
    public CompletableFuture<Void> delete(String path) {
        return StorageUtil.run(ioExecutor, () -> deleteSync(path));
    }

    @Override
    public void deleteSync(String path) {
        try {
            Files.deleteIfExists(Paths.get(path));
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> exists(String path) {
        return CompletableFuture.supplyAsync(
                () -> Files.exists(Paths.get(path)), ioExecutor);
    }

    @Override
    public CompletableFuture<Long> size(AsyncFile file) {
        return StorageUtil.supply(ioExecutor, () -> sizeSync(file));
    }

    @Override
    public long sizeSync(AsyncFile file) {
        try {
            return file.channel.size();
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> mkdir(String path, boolean recursive) {
        return StorageUtil.supply(ioExecutor, () -> {
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
                throw StorageUtil.wrapIOException(e);
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> rmdir(String path, boolean recursive) {
        return StorageUtil.supply(ioExecutor, () -> {
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
                throw StorageUtil.wrapIOException(e);
            }
        });
    }

    @Override
    public CompletableFuture<Void> truncate(AsyncFile file, long size) {
        if (!file.writeMode) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("truncate() requires write mode"));
        }
        return StorageUtil.run(ioExecutor, () -> truncateInternal(file, size));
    }

    @Override
    public void truncateSync(AsyncFile file, long size) {
        if (!file.writeMode) {
            throw new IllegalArgumentException("truncate() requires write mode");
        }
        truncateInternal(file, size);
    }

    private void truncateInternal(AsyncFile file, long size) {
        try {
            long oldSize = file.channel.size();
            file.channel.truncate(size);
            if (!file.atomicReplace) {
                file.channel.position(size);
            }
            if (size < oldSize) {
                file.pendingFsyncBytes = Math.max(0, file.pendingFsyncBytes - (oldSize - size));
            }
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }

    @Override
    public CompletableFuture<Void> close(AsyncFile file) {
        return StorageUtil.run(ioExecutor, () -> closeSync(file));
    }

    @Override
    public void closeSync(AsyncFile file) {
        try {
            file.channel.close();
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        } finally {
            file.onClose.run();
        }
    }

    @Override
    public CompletableFuture<Void> fsync(AsyncFile file) {
        if (!file.writeMode) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("fsync() requires write mode"));
        }
        return StorageUtil.run(ioExecutor, () -> fsyncInternal(file));
    }

    @Override
    public void fsyncSync(AsyncFile file) {
        if (!file.writeMode) {
            throw new IllegalArgumentException("fsync() requires write mode");
        }
        fsyncInternal(file);
    }

    private void fsyncInternal(AbstractStorageFile file) {
        try {
            FileChannel ch = file.currentWriteChannel();
            if (ch != null) ch.force(true);
            file.pendingFsyncBytes = 0;
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }

    @Override
    public CompletableFuture<List<String>> list(String path) {
        return StorageUtil.supply(ioExecutor, () -> {
            String[] names = new File(path).list();
            if (names == null) return Collections.emptyList();
            List<String> filtered = new ArrayList<>(names.length);
            for (String name : names) {
                if (!name.startsWith(TMP_REP_)) filtered.add(name);
            }
            return filtered;
        });
    }

    // ---- AsyncFile helpers ----

    private ByteBuf readFully(FileChannel ch, long length) throws IOException {
        ByteBuf buf = StorageAllocator.ALLOC.directBuffer((int) length);
        try {
            while (buf.writableBytes() > 0) {
                int n = buf.writeBytes(ch, buf.writableBytes());
                if (n < 0) break;
            }
            return buf;
        } catch (Throwable t) {
            buf.release();
            throw t;
        }
    }

    private ByteBuf readFully(FileChannel ch, long length, long offset, long alignSize) throws IOException {
        long alignedStart = alignSize > 0 ? (offset / alignSize) * alignSize : offset;
        long alignedEnd = alignSize > 0 ? ((offset + length + alignSize - 1) / alignSize) * alignSize : offset + length;
        int capacity = (int) (alignedEnd - alignedStart);
        ByteBuf buf = StorageAllocator.ALLOC.directBuffer(capacity);
        try {
            long pos = alignedStart;
            while (buf.writableBytes() > 0) {
                int n = buf.writeBytes(ch, pos, buf.writableBytes());
                if (n < 0) break;
                pos += n;
            }
            // set readerIndex to skip leading alignment padding before offset
            buf.readerIndex((int) (offset - alignedStart));
            return buf;
        } catch (Throwable t) {
            buf.release();
            throw t;
        }
    }

    private long writeFully(FileChannel ch, ByteBuf data) throws IOException {
        int length = data.readableBytes();
        while (data.isReadable()) {
            data.readBytes(ch, data.readableBytes());
        }
        return length;
    }

    private long writeAndFlush(AbstractStorageFile file, ByteBuf data) throws IOException {
        long written = writeFully(file.currentWriteChannel(), data);
        file.pendingFsyncBytes += written;
        if (file.pendingFsyncBytes >= fsyncIntervalBytes) {
            try {
                file.currentWriteChannel().force(true);
                file.pendingFsyncBytes = 0;
            } catch (Throwable t) {
                logger.error("fsync failed for {}", file.identifier(), t);
            }
        }
        return written;
    }

    private boolean maybeSwitchSegment(AsyncSegmentFile file, SegmentDirState s, long nextOffset, long physicalOffset, long bytesRead) throws IOException {
        boolean atSegmentBoundary = nextOffset >= file.openedSegmentEndOffset;
        boolean staleTailEof = bytesRead == 0
                && file.openedSegmentStartOffset != s.lastOffset
                && file.currentSegmentChannel.size() <= physicalOffset;
        if (!atSegmentBoundary && !staleTailEof) {
            return false;
        }
        file.closeCurrent();
        return file.switchToSegment(nextOffset, s);
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
            long expectedLen = 0;
            ByteBuf lenBuf = readFully(tmpCh, 8);
            try {
                int lenRead = lenBuf.readableBytes();
                if (lenRead != 8) {
                    logger.warn("failed to read length from tmp file: read {} bytes, expected 8, deleting {}", lenRead, tmpPath);
                    Files.deleteIfExists(tmpPath);
                    return;
                }
                expectedLen = lenBuf.readLong();
            } finally {
                lenBuf.release();
            }
            long expectedTmpSize = 8 + expectedLen;
            if (tmpSize != expectedTmpSize) {
                logger.warn("tmp file size mismatch: actual {} != expected {}, deleting {}", tmpSize, expectedTmpSize, tmpPath);
                Files.deleteIfExists(tmpPath);
                return;
            }
            ByteBuf dataBuf = readFully(tmpCh, expectedLen);
            try {
                int dataRead = dataBuf.readableBytes();
                if (dataRead != expectedLen) {
                    logger.error("failed to read data from tmp file: read {} bytes, expected {}, deleting {}. This should not happen.",
                        dataRead, expectedLen, tmpPath);
                    Files.deleteIfExists(tmpPath);
                    return;
                }
                fileCh.truncate(0);
                writeFully(fileCh, dataBuf);
            } finally {
                dataBuf.release();
            }
            fileCh.force(true);
            Files.deleteIfExists(tmpPath);
            logger.info("recovered from tmp file: {}", tmpPath);
        }
    }

    private long atomicReplaceWrite(AsyncFile file, ByteBuf data) throws IOException {
        long length = data.readableBytes();
        int savedReaderIndex = data.readerIndex();
        Path tmpPath = getTmpPath(file.path);
        try (FileChannel tmpCh = FileChannel.open(tmpPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuf lenBuf = Unpooled.buffer(8);
            try {
                lenBuf.writeLong(length);
                writeFully(tmpCh, lenBuf);
            } finally {
                lenBuf.release();
            }
            writeFully(tmpCh, data);
            tmpCh.force(true);
        }
        file.channel.truncate(0);
        file.channel.position(0);
        data.readerIndex(savedReaderIndex);
        long written = writeFully(file.channel, data);
        file.channel.force(true);
        Files.deleteIfExists(tmpPath);
        return written;
    }

    // ---- AsyncSegmentFile ----

    @Override
    public CompletableFuture<AsyncSegmentFile> open(String path, String prefix,
            List<String> indexPrefixes, boolean write, String tenant) {
        return StorageUtil.supply(ioExecutor, () -> openSyncInternal(path, prefix, indexPrefixes, write));
    }

    @Override
    public AsyncSegmentFile openSync(String path, String prefix, List<String> indexPrefixes, boolean write, String tenant) {
        return openSyncInternal(path, prefix, indexPrefixes, write);
    }

    private AsyncSegmentFile openSyncInternal(String path, String prefix, List<String> indexPrefixes, boolean write) {
        String key = registryKey(path, prefix);
        DirEntry entry;
        boolean iAmInitializer = false;

        synchronized (lockFor(key)) {
            entry = dirEntries.get(key);
            if (entry == null) {
                entry = new DirEntry();
                dirEntries.put(key, entry);
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
            releaseDirEntry(key, write);
            throw new StorageIOException("init failed for " + key);
        }

        AsyncSegmentFile file = new AsyncSegmentFile(path, prefix, indexPrefixes, key, write);
        boolean success = false;
        try {
            file.openInitialResources(entry.state);
            success = true;
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        } finally {
            if (!success) {
                try {
                    closeSync(file);
                } catch (Throwable t) {
                    logger.error("Failed to close segment file after openInitialResources failure {}", file.key, t);
                }
            }
        }
        return file;
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

    @Override
    public void closeSync(AsyncSegmentFile file) {
        try {
            file.closeCurrent();
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        } finally {
            releaseDirEntry(file.key, file.writeMode);
            file.onClose.run();
        }
    }

    private void releaseDirEntry(String key, boolean write) {
        synchronized (lockFor(key)) {
            DirEntry entry = dirEntries.get(key);
            if (entry == null) return;
            if (write) entry.writerOpen = false;
            if (--entry.refCount == 0) dirEntries.remove(key);
        }
    }

    private DirEntry entryOrThrow(AsyncSegmentFile file) {
        DirEntry entry = dirEntries.get(file.key);
        if (entry == null) throw new IllegalStateException("file is closed: " + file.key);
        return entry;
    }

    @Override
    public CompletableFuture<Void> position(AsyncSegmentFile file, long offset) {
        if (file.writeMode) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("position() is not supported in write mode"));
        }
        return StorageUtil.run(ioExecutor, () -> {
            try {
                file.readPosition = offset;
                SegmentDirState s = entryOrThrow(file).state;
                if (file.switchToSegment(offset, s)) {
                    file.currentSegmentChannel.position(offset - file.openedSegmentStartOffset);
                }
            } catch (IOException e) {
                throw StorageUtil.wrapIOException(e);
            }
        });
    }

    @Override
    public CompletableFuture<ByteBuf> read(AsyncSegmentFile file, long length) {
        return StorageUtil.supply(ioExecutor, () -> {
            try {
                SegmentDirState s = entryOrThrow(file).state;
                if (file.currentSegmentChannel == null) {
                    if (!file.switchToSegment(file.readPosition, s)) return Unpooled.buffer(0);
                    file.currentSegmentChannel.position(file.readPosition - file.openedSegmentStartOffset);
                }
                ByteBuf buf = readFully(file.currentSegmentChannel, length);
                long n = buf.readableBytes();
                file.readPosition += n;
                if (maybeSwitchSegment(file, s, file.readPosition, file.readPosition - file.openedSegmentStartOffset, n)) {
                    file.currentSegmentChannel.position(file.readPosition - file.openedSegmentStartOffset);
                }
                return buf;
            } catch (IOException e) {
                throw StorageUtil.wrapIOException(e);
            }
        });
    }

    @Override
    public CompletableFuture<ByteBuf> read(AsyncSegmentFile file, long length, long offset) {
        return StorageUtil.supply(ioExecutor, () -> {
            try {
                SegmentDirState s = entryOrThrow(file).state;
                if (file.currentSegmentChannel == null
                        || offset < file.openedSegmentStartOffset
                        || offset >= file.openedSegmentEndOffset) {
                    if (!file.switchToSegment(offset, s)) return Unpooled.buffer(0);
                }
                long physicalOffset = offset - file.openedSegmentStartOffset;
                ByteBuf buf = readFully(file.currentSegmentChannel, length, physicalOffset, 0);
                maybeSwitchSegment(file, s, offset + buf.readableBytes(), physicalOffset, buf.readableBytes());
                return buf;
            } catch (IOException e) {
                throw StorageUtil.wrapIOException(e);
            }
        });
    }

    @Override
    public CompletableFuture<Long> write(AsyncSegmentFile file, ByteBuf data) {
        return StorageUtil.supply(ioExecutor, () -> {
            try {
                DirEntry entry = entryOrThrow(file);
                if (entry.state.isEmpty()) {
                    file.openFirstSegmentChannelForWrite(entry);
                }
                return writeAndFlush(file, data);
            } catch (IOException e) {
                throw StorageUtil.wrapIOException(e);
            } finally {
                data.release();
            }
        });
    }

    @Override
    public CompletableFuture<Map<String, AsyncFile>> roll(AsyncSegmentFile file) {
        if (!file.writeMode) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("roll() requires write mode"));
        }
        return StorageUtil.supply(ioExecutor, () -> {
            try {
                DirEntry entry = entryOrThrow(file);
                return file.roll(entry);
            } catch (IOException e) {
                throw StorageUtil.wrapIOException(e);
            }
        });
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
        return StorageUtil.supply(ioExecutor, () -> {
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
                throw StorageUtil.wrapIOException(e);
            }
        });
    }

    @Override
    public CompletableFuture<Map<String, AsyncFile>> getCurrentIndexFiles(AsyncSegmentFile file) {
        return getCurrentIndexFiles(file, file.indexPrefixes);
    }

    @Override
    public CompletableFuture<Long> size(AsyncSegmentFile file) {
        return StorageUtil.supply(ioExecutor, () -> {
            try {
                SegmentDirState s = entryOrThrow(file).state;
                if (s.isEmpty()) {
                    return 0L;
                }
                return file.exclusiveEndOffset(s.lastOffset) - s.firstOffset;
            } catch (IOException e) {
                throw StorageUtil.wrapIOException(e);
            }
        });
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
        return StorageUtil.supply(ioExecutor, () -> {
            try {
                return Files.getLastModifiedTime(file.segmentPath(startOffset)).toMillis();
            } catch (IOException e) {
                throw StorageUtil.wrapIOException(e);
            }
        });
    }

    @Override
    public CompletableFuture<Long> sizeOfSegment(AsyncSegmentFile file, long startOffset) {
        return StorageUtil.supply(ioExecutor, () -> {
            try {
                return Files.size(file.segmentPath(startOffset));
            } catch (IOException e) {
                throw StorageUtil.wrapIOException(e);
            }
        });
    }

    @Override
    public CompletableFuture<Void> deleteSegments(AsyncSegmentFile file, List<Long> startOffsets) {
        if (!file.writeMode) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("deleteSegments() requires write mode"));
        }
        return StorageUtil.run(ioExecutor, () -> {
            try {
                DirEntry entry = entryOrThrow(file);
                file.deleteSegments(startOffsets, entry);
            } catch (IOException e) {
                throw StorageUtil.wrapIOException(e);
            }
        });
    }

    @Override
    public CompletableFuture<Map<String, AsyncFile>> truncate(AsyncSegmentFile file, long offset) {
        if (!file.writeMode) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("truncate() requires write mode"));
        }
        return StorageUtil.supply(ioExecutor, () -> {
            try {
                DirEntry entry = entryOrThrow(file);
                return file.truncate(offset, entry);
            } catch (IOException e) {
                throw StorageUtil.wrapIOException(e);
            }
        });
    }

    @Override
    public CompletableFuture<Void> close(AsyncSegmentFile file) {
        return StorageUtil.run(ioExecutor, () -> closeSync(file));
    }

    @Override
    public CompletableFuture<Void> delete(AsyncSegmentFile file) {
        if (!file.writeMode) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("delete() requires write mode"));
        }
        return StorageUtil.run(ioExecutor, () -> {
            try {
                DirEntry entry = entryOrThrow(file);
                file.delete(entry);
            } catch (IOException e) {
                throw StorageUtil.wrapIOException(e);
            }
        });
    }

    @Override
    public CompletableFuture<Void> fsync(AsyncSegmentFile file) {
        if (!file.writeMode) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("fsync() requires write mode"));
        }
        return StorageUtil.run(ioExecutor, () -> fsyncInternal(file));
    }

    @Override
    public CompletableFuture<Long> transferTo(AsyncFile file, long position, long count,
            WritableByteChannel target) {
        return StorageUtil.supply(ioExecutor, () -> {
            try {
                return file.channel.transferTo(position, count, target);
            } catch (ClosedChannelException e) {
                if (!target.isOpen()) throw new SocketErrorException(e);
                throw StorageUtil.wrapIOException(e);
            } catch (IOException e) {
                throw StorageUtil.wrapIOException(e);
            }
        });
    }

    @Override
    public CompletableFuture<Long> transferTo(AsyncSegmentFile file, long offset, long count,
            WritableByteChannel target) {
        return StorageUtil.supply(ioExecutor, () -> {
            try {
                SegmentDirState s = entryOrThrow(file).state;
                if (file.currentSegmentChannel == null
                        || offset < file.openedSegmentStartOffset
                        || offset >= file.openedSegmentEndOffset) {
                    if (!file.switchToSegment(offset, s)) return 0L;
                }
                long physicalOffset = offset - file.openedSegmentStartOffset;
                long n = file.currentSegmentChannel.transferTo(physicalOffset, count, target);
                maybeSwitchSegment(file, s, offset + n, physicalOffset, n);
                return n;
            } catch (ClosedChannelException e) {
                if (!target.isOpen()) throw new SocketErrorException(e);
                throw StorageUtil.wrapIOException(e);
            } catch (IOException e) {
                throw StorageUtil.wrapIOException(e);
            }
        });
    }
}

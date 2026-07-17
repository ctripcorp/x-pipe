package com.ctrip.xpipe.redis.keeper.storage;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
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
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.tuple.Pair;

// low level file system implementation, not used directly.
// TFS guarantees that metadata is synchronously delivered: create/rm operations are durable after returning.
// TFS guarantees that close will flush all data before returning.
public class AsyncTFSBasedFileSystem implements AsyncFileSystem {

    private static final Logger logger = LoggerFactory.getLogger(AsyncTFSBasedFileSystem.class);
    private static final String TMP_REP_ = "TMP_REP_";
    private static final int LOCK_STRIPES = 32;

    private final ExecutorService ioExecutor;
    private final long fsyncIntervalBytes;

    // Registry of shared file state, keyed by file key.
    private final ConcurrentHashMap<String, FileEntry> fileEntries = new ConcurrentHashMap<>();

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

    // Translates a checked IOException into a runtime exception that reflects
    // recovery semantics: StaleStateException for mismatched state, StorageIOException for
    // genuine transient IO failures.IllegalArgumentExceptions for invalid arguments.
    // ---- AsyncFile ----

    // atomicReplace uses tmp file approach instead of rename because tfs currently does not support rename.
    // Tmp file format: [8-byte length][data].

    @Override
    public AsyncFile openSync(String path, AbstractStorageFile.OpenMode openMode, boolean atomicReplace, boolean lenient, String tenant) {
        String key = path;
        Path p = Paths.get(path);
        return openWithFileEntry(key, openMode.canWrite(),
                entry -> {
                    if (atomicReplace) {
                        try {
                            recoverFromTmp(p);
                        } catch (IOException e) {
                            throw StorageUtil.wrapIOException(e);
                        }
                    }
                },
                () -> new AsyncFile(path, atomicReplace, openMode),
                (file, entry) -> {
                    if (lenient && Files.exists(p) && !Files.isRegularFile(p)) {
                        return;
                    }
                    try {
                        file.openCurrentChannel();
                    } catch (IOException e) {
                        throw StorageUtil.wrapIOException(e);
                    }
                },
                this::closeSync);
    }

    private Pair<Boolean, FileEntry> acquireFileEntry(String key, boolean write) {
        synchronized (lockFor(key)) {
            FileEntry entry = fileEntries.get(key);
            boolean first = false;
            if (entry == null) {
                entry = new FileEntry();
                fileEntries.put(key, entry);
                first = true;
            } else if (write && entry.writerOpen) {
                throw new IllegalStateException("writer already open for " + key);
            }
            if (write) {
                entry.writerOpen = true;
            }
            entry.refCount++;
            return new Pair<>(first, entry);
        }
    }

    private void releaseFileEntry(String key, boolean write) {
        synchronized (lockFor(key)) {
            FileEntry entry = fileEntries.get(key);
            if (entry == null) return;
            if (write) entry.writerOpen = false;
            if (--entry.refCount == 0) fileEntries.remove(key);
        }
    }

    private <T extends AbstractStorageFile> T openWithFileEntry(String key, boolean write,
            Consumer<FileEntry> initAction, Supplier<T> fileFactory,
            BiConsumer<T, FileEntry> openAction, Consumer<T> cleanupAction) {
        Pair<Boolean, FileEntry> acquired = acquireFileEntry(key, write);
        boolean iAmInitializer = acquired.getKey();
        FileEntry entry = acquired.getValue();

        if (iAmInitializer) {
            try {
                initAction.accept(entry);
            } catch (Throwable t) {
                logger.error("Failed to initialize file entry {}", key, t);
                entry.initFailed = true;
            } finally {
                entry.initDone.countDown();
            }
        } else {
            try {
                entry.initDone.await();
            } catch (InterruptedException e) {
                releaseFileEntry(key, write);
                throw new RuntimeException(e);
            }
        }
        if (entry.initFailed) {
            releaseFileEntry(key, write);
            throw new StorageIOException("init failed for " + key);
        }

        T file = fileFactory.get();
        boolean success = false;
        try {
            openAction.accept(file, entry);
            success = true;
            return file;
        } finally {
            if (!success) {
                try {
                    cleanupAction.accept(file);
                } catch (Throwable t) {
                    logger.error("Failed to cleanup opened file {}", key, t);
                }
            }
        }
    }

    @Override
    public CompletableFuture<Boolean> isFile(AsyncFile file) {
        return StorageUtil.supply(ioExecutor, () -> Files.isRegularFile(Paths.get(file.path)));
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
    public void positionSync(AsyncFile file, long position) {
        StorageUtil.requireOpen(file);
        file.position = position;
    }


    @Override
    public ByteBuf readSync(AsyncFile file, long length, long offset, long alignSize) {
        StorageUtil.requireOpen(file);
        try {
            return readFully(file.channel, length, offset, alignSize);
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }



    @Override
    public long writeSync(AsyncFile file, ByteBuf data) {
        StorageUtil.requireOpen(file);
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
    public long sizeSync(AsyncFile file) {
        StorageUtil.requireOpen(file);
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
    public void truncateSync(AsyncFile file, long size) {
        StorageUtil.requireOpen(file);
        try {
            long oldSize = file.channel.size();
            if (size >= oldSize) {
                return;
            }
            file.channel.truncate(size);
            if (!file.atomicReplace) {
                file.channel.position(size);
            }
            if (size < oldSize) {
                file.pendingFsyncBytes = Math.max(0, file.pendingFsyncBytes - (oldSize - size));
            }
            fsyncInternal(file);
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }


    @Override
    public void closeSync(AsyncFile file) {
        if (file.closed) {
            return;
        }
        file.closed = true;
        try {
            if (file.channel != null) {
                file.channel.close();
            }
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        } finally {
            releaseFileEntry(file.path, file.canWrite());
        }
    }


    @Override
    public void fsyncSync(AsyncFile file) {
        StorageUtil.requireOpen(file);
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

    private void maybeSwitchSegment(AsyncSegmentFile file, SegmentDirState s, long bytesRead) {
        long nextOffset = file.position;
        long physicalOffset = nextOffset - file.openedSegmentStartOffset;
        boolean atSegmentBoundary = nextOffset >= file.openedSegmentEndOffset;
        try {
            boolean staleTailEof = bytesRead == 0
                    && file.openedSegmentStartOffset != s.lastOffset
                    && file.currentSegmentChannel.size() <= physicalOffset;
            if (!atSegmentBoundary && !staleTailEof) {
                return;
            }
            // Close first to trigger recalculation of opened range.
            // Prefetch only; next read recovers via ensureSegmentOpenForRead if this fails.
            file.closeCurrent();
            if (!file.switchToSegment(nextOffset, s)) {
                return;
            }
            file.openSegmentChannelForRead();
        } catch (IOException e) {
            logger.error("maybeSwitchSegment failed for {} at position {}, will retry on next read",
                    file.identifier(), nextOffset, e);
        }
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
            ByteBuf lenBuf = readFully(tmpCh, 8, 0, 0);
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
            ByteBuf dataBuf = readFully(tmpCh, expectedLen, 8, 0);
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
    public AsyncSegmentFile openSync(String path, String prefix, List<String> indexPrefixes, boolean write, String tenant) {
        String key = StorageUtil.segmentKey(path, prefix);
        return openWithFileEntry(key, write,
                entry -> {
                    try {
                        initFromDisk(entry, path, prefix, indexPrefixes);
                    } catch (IOException e) {
                        throw StorageUtil.wrapIOException(e);
                    }
                },
                () -> new AsyncSegmentFile(path, prefix, indexPrefixes, key, write),
                (file, entry) -> {
                    try {
                        file.openInitialResources(entry.state);
                    } catch (IOException e) {
                        throw StorageUtil.wrapIOException(e);
                    }
                },
                file -> closeSync(file));
    }

    private void initFromDisk(FileEntry entry, String path, String prefix, List<String> indexPrefixes) throws IOException {
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
        if (file.closed) {
            return;
        }
        file.closed = true;
        try {
            file.closeCurrent();
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        } finally {
            releaseFileEntry(file.key, file.canWrite());
        }
    }

    private FileEntry entryOrThrow(AsyncSegmentFile file) {
        FileEntry entry = fileEntries.get(file.key);
        if (entry == null) throw new IllegalStateException("file is closed: " + file.key);
        return entry;
    }

    private boolean ensureSegmentOpenForRead(AsyncSegmentFile file, long offset, SegmentDirState s)
            throws IOException {
        if (!file.switchToSegment(offset, s)) {
            return false;
        }
        if (file.currentSegmentChannel == null) {
            file.openSegmentChannelForRead();
        }
        return true;
    }


    @Override
    public void positionSync(AsyncSegmentFile file, long offset) {
        StorageUtil.requireOpen(file);
        try {
            file.position = offset;
            SegmentDirState s = entryOrThrow(file).state;
            file.switchToSegment(offset, s);
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }


    @Override
    public ByteBuf readSync(AsyncSegmentFile file, long length) {
        StorageUtil.requireOpen(file);
        try {
            SegmentDirState s = entryOrThrow(file).state;
            if (!ensureSegmentOpenForRead(file, file.position, s)) return Unpooled.buffer(0);
            long physicalOffset = file.position - file.openedSegmentStartOffset;
            ByteBuf buf = readFully(file.currentSegmentChannel, length, physicalOffset, 0);
            long n = buf.readableBytes();
            file.position += n;
            maybeSwitchSegment(file, s, n);
            return buf;
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }


    @Override
    public ByteBuf readSync(AsyncSegmentFile file, long length, long offset) {
        StorageUtil.requireOpen(file);
        try {
            SegmentDirState s = entryOrThrow(file).state;
            if (!ensureSegmentOpenForRead(file, offset, s)) return Unpooled.buffer(0);
            long physicalOffset = offset - file.openedSegmentStartOffset;
            ByteBuf buf = readFully(file.currentSegmentChannel, length, physicalOffset, 0);
            long n = buf.readableBytes();
            file.position = offset + n;
            maybeSwitchSegment(file, s, n);
            return buf;
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }


    @Override
    public long writeSync(AsyncSegmentFile file, ByteBuf data) {
        try {
            StorageUtil.requireOpen(file);
            FileEntry entry = entryOrThrow(file);
            if (entry.state.isEmpty()) {
                file.openFirstSegmentChannelForWrite(entry);
            }
            return writeAndFlush(file, data);
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        } finally {
            data.release();
        }
    }


    @Override
    public Map<String, AsyncFile> rollSync(AsyncSegmentFile file) {
        StorageUtil.requireOpen(file);
        try {
            FileEntry entry = entryOrThrow(file);
            return file.roll(entry);
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }

    @Override
    public List<Long> list(AsyncSegmentFile file) {
        return entryOrThrow(file).state.offsets();
    }

    @Override
    public long getCurrentSegmentStartOffset(AsyncSegmentFile file) {
        return file.openedSegmentStartOffset;
    }


    @Override
    public Map<String, AsyncFile> getCurrentIndexFilesSync(AsyncSegmentFile file, List<String> indexPrefixes) {
        StorageUtil.requireOpen(file);
        try {
            FileEntry entry = entryOrThrow(file);
            SegmentDirState s = entry.state;
            if (s.isEmpty()) {
                if (file.canWrite()) {
                    file.openFirstSegmentChannelForWrite(entry);
                } else {
                    return new HashMap<String, AsyncFile>();
                }
            } else if (!file.canWrite()) {
                if (!file.switchToSegment(file.position, s)) {
                    return new HashMap<String, AsyncFile>();
                }
            }
            return file.getCurrentIndexFiles(indexPrefixes);
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }


    @Override
    public Map<String, AsyncFile> getCurrentIndexFilesSync(AsyncSegmentFile file) {
        return getCurrentIndexFilesSync(file, file.indexPrefixes);
    }


    @Override
    public long sizeSync(AsyncSegmentFile file) {
        StorageUtil.requireOpen(file);
        SegmentDirState s = entryOrThrow(file).state;
        if (s.isEmpty()) {
            return 0L;
        }
        try {
            return file.exclusiveEndOffset(s.lastOffset) - s.firstOffset;
        } catch (NoSuchFileException e) {
            return s.lastOffset - s.firstOffset;
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
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
            } catch (NoSuchFileException e) {
                return 0L;
            } catch (IOException e) {
                throw StorageUtil.wrapIOException(e);
            }
        });
    }


    @Override
    public long sizeOfSegmentSync(AsyncSegmentFile file, long startOffset) {
        try {
            return Files.size(file.segmentPath(startOffset));
        } catch (NoSuchFileException e) {
            return 0L;
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }


    @Override
    public void deleteSegmentsSync(AsyncSegmentFile file, List<Long> startOffsets) {
        StorageUtil.requireOpen(file);
        try {
            FileEntry entry = entryOrThrow(file);
            file.deleteSegments(startOffsets, entry);
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }


    @Override
    public Map<String, AsyncFile> truncateSync(AsyncSegmentFile file, long offset) {
        StorageUtil.requireOpen(file);
        try {
            FileEntry entry = entryOrThrow(file);
            Map<String, AsyncFile> result = file.truncate(offset, entry);
            fsyncInternal(file);
            return result;
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }


    @Override
    public CompletableFuture<Void> delete(AsyncSegmentFile file) {
        return StorageUtil.run(ioExecutor, () -> {
            StorageUtil.requireOpen(file);
            try {
                FileEntry entry = entryOrThrow(file);
                file.delete(entry);
            } catch (IOException e) {
                throw StorageUtil.wrapIOException(e);
            }
        });
    }


    @Override
    public void fsyncSync(AsyncSegmentFile file) {
        StorageUtil.requireOpen(file);
        fsyncInternal(file);
    }


    @Override
    public long transferToSync(AsyncFile file, long position, long count, WritableByteChannel target) {
        StorageUtil.requireOpen(file);
        try {
            return file.channel.transferTo(position, count, target);
        } catch (ClosedChannelException e) {
            if (!target.isOpen()) throw new SocketErrorException(e);
            throw StorageUtil.wrapIOException(e);
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }


    @Override
    public long transferToSync(AsyncSegmentFile file, long offset, long count, WritableByteChannel target) {
        StorageUtil.requireOpen(file);
        try {
            SegmentDirState s = entryOrThrow(file).state;
            if (!ensureSegmentOpenForRead(file, offset, s)) return 0L;
            long physicalOffset = offset - file.openedSegmentStartOffset;
            long n = file.currentSegmentChannel.transferTo(physicalOffset, count, target);
            file.position = offset + n;
            maybeSwitchSegment(file, s, n);
            return n;
        } catch (ClosedChannelException e) {
            if (!target.isOpen()) throw new SocketErrorException(e);
            throw StorageUtil.wrapIOException(e);
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }
}

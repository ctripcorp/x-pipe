package com.ctrip.xpipe.redis.keeper.storage;

import java.io.IOException;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
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
    // Size of the length header a tmp file carries in front of its data.
    private static final int TMP_HEADER_BYTES = 8;
    // Returned by openValidTmpSync when there is nothing usable to read from the tmp file.
    private static final Pair<Long, FileChannel> NO_VALID_TMP = new Pair<>(-1L, null);

    private final ExecutorService ioExecutor;
    private volatile long fsyncIntervalBytes;
    private volatile long fsyncIntervalNanos;

    // Registry of shared file state, keyed by file key.
    private final ConcurrentHashMap<String, FileEntry> fileEntries = new ConcurrentHashMap<>();

    private final Object[] openCloseLocks = new Object[LOCK_STRIPES];

    public AsyncTFSBasedFileSystem(ExecutorService ioExecutor, long fsyncIntervalBytes, long fsyncIntervalMillis) {
        this.ioExecutor = ioExecutor;
        this.fsyncIntervalBytes = fsyncIntervalBytes;
        this.fsyncIntervalNanos = fsyncIntervalMillis * 1_000_000L;
        for (int i = 0; i < LOCK_STRIPES; i++) openCloseLocks[i] = new Object();
    }

    public long getFsyncIntervalBytes() {
        return fsyncIntervalBytes;
    }

    public void setFsyncIntervalBytes(long fsyncIntervalBytes) {
        this.fsyncIntervalBytes = fsyncIntervalBytes;
    }

    public long getFsyncIntervalMillis() {
        return fsyncIntervalNanos / 1_000_000L;
    }

    public void setFsyncIntervalMillis(long fsyncIntervalMillis) {
        this.fsyncIntervalNanos = fsyncIntervalMillis * 1_000_000L;
    }

    @Override
    public void shutdown() {
        ioExecutor.shutdown();
    }

    private Object lockFor(String key) {
        return openCloseLocks[(key.hashCode() & 0x7fffffff) % LOCK_STRIPES];
    }

    // An atomic replace uses the tmp file approach instead of rename because tfs currently does not
    // support rename. Tmp file format: [8-byte length][data].

    @Override
    public AsyncFile openSync(String path, String key, String ioKey,
            AbstractStorageFile.OpenMode openMode, AbstractStorageFile.ReplaceMode replaceMode,
            boolean lenient, String tenant, boolean noFs) {
        AsyncFile file = new AsyncFile(path, replaceMode, openMode, key, ioKey, lenient);
        file.needPrepare = noFs;
        return file;
    }

    @Override
    public boolean openWithFileEntry(AsyncFile file, boolean noFs,
            BiConsumer<String, CompletableFuture<?>> register,
            BiConsumer<String, List<FileChannel>> clean,
            long recoverTimeoutMs, long ioTimeoutMs) {
        final Path p = Paths.get(file.path);
        final Function<FileEntry, Boolean> initAction = noFs || !file.isAtomicReplace()
                ? entry -> true
                : entry -> StorageUtil.awaitIoCachePrep(ioExecutor, file, file.key, recoverTimeoutMs, register,
                        () -> recoverAtomicReplaceSync(p, file.canWrite()), null);
        final OpenAction<AsyncFile> openAction = noFs
                ? (f, entry, first) -> { }
                : (f, entry, first) -> StorageUtil.awaitIoCachePrep(ioExecutor, f, null, ioTimeoutMs, register,
                        () -> {
                            // always try to recover unless already done in init(in other words, the writer is the initializer).
                            if (f.isAtomicReplace() && f.canWrite() && !first) {
                                recoverAtomicReplaceSync(p, true);
                                entry.initialized = true;
                            }
                            openChannelIfNeeded(f, p);
                            return null;
                        }, null);
        return openWithFileEntry(file, initAction, openAction,
                f -> clean.accept(f.path, closeSync(f)));
    }

    // Returns true if the file is already recovered or no recovery is needed.
    private boolean recoverAtomicReplaceSync(Path filePath, boolean canWrite) {
        Path tmpPath = getTmpPath(filePath.toString());
        try {
            Pair<Long, FileChannel> tmp = openValidTmpSync(filePath.toString());
            long expectedLen = tmp.getKey();
            if (expectedLen < 0) {
                if (canWrite && Files.deleteIfExists(tmpPath)) {
                    logger.warn("deleted incomplete tmp file {}", tmpPath);
                }
                return true;
            }
            try (FileChannel tmpCh = tmp.getValue()) {
                if (!canWrite) {
                    logger.info("valid tmp file {} left behind, recovery deferred to the first writer", tmpPath);
                    return false;
                }
                try (FileChannel fileCh = FileChannel.open(filePath,
                        StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
                    ByteBuf dataBuf = readFully(tmpCh, expectedLen, TMP_HEADER_BYTES, 0);
                    try {
                        int dataRead = dataBuf.readableBytes();
                        if (dataRead != expectedLen) {
                            logger.error("failed to read data from tmp file: read {} bytes, expected {}, deleting {}. This should not happen.",
                                dataRead, expectedLen, tmpPath);
                            Files.deleteIfExists(tmpPath);
                            return true;
                        }
                        fileCh.truncate(0);
                        writeFully(fileCh, dataBuf);
                    } finally {
                        dataBuf.release();
                    }
                    fileCh.force(true);
                }
                Files.deleteIfExists(tmpPath);
                logger.info("recovered from tmp file: {}", tmpPath);
                return true;
            }
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }

    // Checks the [length][data] layout of an open tmp file and returns the data length, or -1 when
    // the tmp file is not a complete atomic replace, meaning it must be discarded rather than used.
    // tmpPath is only for logging.
    private long tmpDataLength(FileChannel tmpCh, Path tmpPath) throws IOException {
        long tmpSize = tmpCh.size();
        if (tmpSize < TMP_HEADER_BYTES) {
            logger.warn("tmp file size too small: {} < {}, invalid tmp file {}", tmpSize, TMP_HEADER_BYTES, tmpPath);
            return -1;
        }
        long dataLength;
        ByteBuf lenBuf = readFully(tmpCh, TMP_HEADER_BYTES, 0, 0);
        try {
            int lenRead = lenBuf.readableBytes();
            if (lenRead != TMP_HEADER_BYTES) {
                logger.warn("failed to read length from tmp file: read {} bytes, expected {}, invalid tmp file {}",
                        lenRead, TMP_HEADER_BYTES, tmpPath);
                return -1;
            }
            dataLength = lenBuf.readLong();
        } finally {
            lenBuf.release();
        }
        long expectedTmpSize = TMP_HEADER_BYTES + dataLength;
        if (tmpSize != expectedTmpSize) {
            logger.warn("tmp file size mismatch: actual {} != expected {}, invalid tmp file {}",
                    tmpSize, expectedTmpSize, tmpPath);
            return -1;
        }
        return dataLength;
    }

    private boolean tmpFirst(AsyncFile file) {
        return file.preferTmp();
    }

    // Returns the data length and the open channel of a valid tmp file, or NO_VALID_TMP.
    private Pair<Long, FileChannel> openValidTmpSync(String filePath) throws IOException {
        Path tmpPath = getTmpPath(filePath);
        if (!StorageUtil.existsSync(tmpPath)) {
            return NO_VALID_TMP;
        }
        FileChannel tmpCh;
        try {
            tmpCh = FileChannel.open(tmpPath, StandardOpenOption.READ);
        } catch (NoSuchFileException e) {
            logger.info("tmp file {} is gone between exists and open, using the target file {}",
                    tmpPath, filePath);
            return NO_VALID_TMP;
        }
        boolean valid = false;
        try {
            long dataLength = tmpDataLength(tmpCh, tmpPath);
            valid = dataLength >= 0;
            return valid ? new Pair<>(dataLength, tmpCh) : NO_VALID_TMP;
        } finally {
            if (!valid) {
                StorageUtil.closeChannels(Collections.singletonList(tmpCh));
            }
        }
    }

    private void openChannelIfNeeded(AsyncFile file, Path p) {
        try {
            if (file.lenient && StorageUtil.existsSync(p) && !StorageUtil.isRegularFileSync(p)) {
                return;
            }
            file.openCurrentChannel();
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }

    private Pair<Boolean, FileEntry> acquireFileEntry(String key, String path, boolean write) {
        synchronized (lockFor(key)) {
            FileEntry entry = fileEntries.get(key);
            boolean first = false;
            if (entry == null) {
                entry = new FileEntry();
                fileEntries.put(key, entry);
                first = true;
            } else if (write && entry.writerOpen) {
                throw new IllegalStateException("writer already open for " + path);
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

    @FunctionalInterface
    private interface OpenAction<T> {
        void accept(T file, FileEntry entry, boolean first);
    }

    private <T extends AbstractStorageFile> boolean openWithFileEntry(T file,
            Function<FileEntry, Boolean> initAction,
            OpenAction<T> openAction, Consumer<T> cleanupAction) {
        final String key = file.key;
        final String path = file.path;
        final boolean write = file.canWrite();
        Pair<Boolean, FileEntry> acquired = acquireFileEntry(key, path, write);
        boolean iAmInitializer = acquired.getKey();
        FileEntry entry = acquired.getValue();

        if (iAmInitializer) {
            try {
                entry.initialized = initAction.apply(entry);
            } catch (Throwable t) {
                logger.error("Failed to initialize file entry {}", path, t);
                entry.initError = t;
            } finally {
                entry.initDone.countDown();
            }
        } else {
            try {
                entry.initDone.await();
            } catch (InterruptedException e) {
                releaseFileEntry(key, write);
                throw new OperationNotExecutedException(path, e);
            }
        }
        if (entry.initError != null) {
            releaseFileEntry(key, write);
            if (entry.initError instanceof OperationNotExecutedException) {
                throw (OperationNotExecutedException) entry.initError;
            }
            throw new StorageIOException("init failed for " + path, entry.initError);
        }

        boolean success = false;
        try {
            openAction.accept(file, entry, iAmInitializer);
            success = true;
            return entry.initialized;
        } finally {
            if (!success) {
                try {
                    cleanupAction.accept(file);
                } catch (Throwable t) {
                    logger.error("Failed to cleanup opened file {}", path, t);
                }
            }
        }
    }

    @Override
    public CompletableFuture<Boolean> isFile(AsyncFile file) {
        return StorageUtil.supply(ioExecutor, () -> {
            try {
                return StorageUtil.isRegularFileSync(Paths.get(file.path));
            } catch (IOException e) {
                throw StorageUtil.wrapIOException(e);
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> isDirectory(String path) {
        return StorageUtil.supply(ioExecutor, () -> {
            try {
                return StorageUtil.isDirectorySync(Paths.get(path));
            } catch (IOException e) {
                throw StorageUtil.wrapIOException(e);
            }
        });
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
        // A whole-file replace is never read with alignment, which is what lets the tmp read below
        // drop alignSize. Rejected rather than asserted so a future aligned caller fails loudly
        // instead of silently getting a buffer that does not honour the alignment contract.
        if (file.isAtomicReplace() && alignSize != 0) {
            throw new IllegalArgumentException(
                    "atomic replace read must not be aligned: " + file.path + ", alignSize=" + alignSize);
        }
        try {
            if (tmpFirst(file)) {
                Pair<Long, FileChannel> tmp = openValidTmpSync(file.path);
                if (tmp.getKey() >= 0) {
                    try (FileChannel tmpCh = tmp.getValue()) {
                        return readFully(tmpCh, length, TMP_HEADER_BYTES + offset, 0);
                    }
                }
            }
            return readFully(file.channel, length, offset, alignSize);
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }



    @Override
    public long writeSync(AsyncFile file, ByteBuf data) {
        try {
            StorageUtil.requireOpen(file);
            if (file.isAtomicReplace()) {
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
        return StorageUtil.supply(ioExecutor, () -> {
            try {
                return StorageUtil.existsSync(Paths.get(path));
            } catch (IOException e) {
                throw StorageUtil.wrapIOException(e);
            }
        });
    }


    @Override
    public long sizeSync(AsyncFile file) {
        StorageUtil.requireOpen(file);
        try {
            if (tmpFirst(file)) {
                Pair<Long, FileChannel> tmp = openValidTmpSync(file.path);
                if (tmp.getKey() >= 0) {
                    StorageUtil.closeChannels(Collections.singletonList(tmp.getValue()));
                    return tmp.getKey();
                }
            }
            return file.channel.size();
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }

    @Override
    public boolean mkdirSync(String path, boolean recursive) {
        try {
            if (recursive) {
                Files.createDirectories(Paths.get(path));
            } else {
                try {
                    Files.createDirectory(Paths.get(path));
                } catch (FileAlreadyExistsException e) {
                    if (!StorageUtil.isDirectorySync(Paths.get(path))) {
                        throw e;
                    }
                }
            }
            return true;
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> rmdir(String path, boolean recursive) {
        return StorageUtil.supply(ioExecutor, () -> {
            try {
                Path dir = Paths.get(path);
                if (!StorageUtil.existsSync(dir)) return true;
                if (!StorageUtil.isDirectorySync(dir)) throw new IllegalArgumentException("not a directory: " + path);
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
            if (!file.isAtomicReplace()) {
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

    private List<FileChannel> closeSyncInternal(AbstractStorageFile file) {
        final List<FileChannel> detached;
        synchronized (file) {
            if (file.closed) {
                return Collections.emptyList();
            }
            file.closed = true;
            detached = file.detachCurrentChannels();
        }
        releaseFileEntry(file.key, file.canWrite());
        return detached;
    }

    @Override
    public List<FileChannel> closeSync(AsyncFile file) {
        return closeSyncInternal(file);
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
            file.lastFsyncNanos = System.nanoTime();
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }

    @Override
    public CompletableFuture<List<String>> list(String path) {
        return StorageUtil.supply(ioExecutor, () -> {
            final List<String> names;
            try {
                names = StorageUtil.listNamesSync(Paths.get(path));
            } catch (IOException e) {
                throw StorageUtil.wrapIOException(e);
            }
            List<String> filtered = new ArrayList<>(names.size());
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
        ByteBuf buf;
        try {
            buf = StorageAllocator.ALLOC.directBuffer(capacity);
        } catch (Throwable e) {
            throw new CacheMemoryReserveException(capacity, e);
        }
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
        if (file.pendingFsyncBytes >= fsyncIntervalBytes
                || System.nanoTime() - file.lastFsyncNanos >= fsyncIntervalNanos) {
            try {
                file.currentWriteChannel().force(true);
                file.pendingFsyncBytes = 0;
                file.lastFsyncNanos = System.nanoTime();
            } catch (Throwable t) {
                logger.error("fsync failed for {}", file.path, t);
            }
        }
        return written;
    }

    private void maybeSwitchSegment(AsyncSegmentFile file, SegmentDirState s, long bytesRead, long offset) {
        long nextOffset = offset;
        long physicalOffset = nextOffset - file.openedSegmentStartOffset;
        boolean atSegmentBoundary = nextOffset >= file.openedSegmentEndOffset;
        try {
            boolean staleTailEof = bytesRead == 0
                    && file.openedSegmentStartOffset != s.lastOffset
                    && file.currentSegmentChannel.size() <= physicalOffset;
            if (!atSegmentBoundary && !staleTailEof) {
                return;
            }
            // Null the channel so the next read's will re-switch first.
            if (file.currentSegmentChannel != null) {
                try {
                    file.currentSegmentChannel.close();
                } finally {
                    file.currentSegmentChannel = null;
                }
            }
        } catch (IOException e) {
            logger.error("maybeSwitchSegment failed for {} at position {}, will retry on next read",
                    file.path, nextOffset, e);
        }
    }

    private Path getTmpPath(String filePath) {
        Path p = Paths.get(filePath);
        return Paths.get(p.getParent().toString(), TMP_REP_ + p.getFileName());
    }

    // Whole-file replace is durable after force; pendingFsyncBytes / lastFsyncNanos are unused on this path.
    private long atomicReplaceWrite(AsyncFile file, ByteBuf data) throws IOException {
        long length = data.readableBytes();
        Path tmpPath = getTmpPath(file.path);
        try (FileChannel tmpCh = FileChannel.open(tmpPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuf lenBuf = Unpooled.buffer(TMP_HEADER_BYTES);
            try {
                lenBuf.writeLong(length);
                writeFully(tmpCh, lenBuf);
            } finally {
                lenBuf.release();
            }
            writeFully(tmpCh, data.duplicate());
            tmpCh.force(true);
        }
        file.channel.truncate(0);
        file.channel.position(0);
        long written = writeFully(file.channel, data);
        file.channel.force(true);
        Files.deleteIfExists(tmpPath);
        return written;
    }

    // ---- AsyncSegmentFile ----


    @Override
    public AsyncSegmentFile openSync(String path, String prefix, String key, String ioKey,
            List<String> indexPrefixes, boolean write, String tenant, boolean noFs) {
        AsyncSegmentFile file = new AsyncSegmentFile(path, prefix, indexPrefixes, key, ioKey, write);
        file.needPrepare = noFs;
        return file;
    }

    @Override
    public boolean openWithFileEntry(AsyncSegmentFile file, boolean noFs,
            BiConsumer<String, CompletableFuture<?>> register,
            BiConsumer<String, List<FileChannel>> clean,
            long recoverTimeoutMs, long ioTimeoutMs) {
        final Function<FileEntry, Boolean> initAction = noFs
                ? entry -> true
                : entry -> StorageUtil.awaitIoCachePrep(ioExecutor, file, file.key, recoverTimeoutMs, register,
                        () -> {
                            initFromDiskSync(entry, file.dirPath, file.prefix, file.indexPrefixes,
                                    file.canWrite());
                            return true;
                        }, null);
        final OpenAction<AsyncSegmentFile> openAction = (f, entry, first) -> {
            f.openInitialResources(entry);
            if (!noFs) {
                StorageUtil.awaitIoCachePrep(ioExecutor, f, null, ioTimeoutMs, register, () -> {
                    // always try to delete orphan files unless already done in init(in other words, the writer is the initializer).
                    if (f.canWrite() && !first) {
                        deleteOrphanSegmentFilesSync(f);
                    }
                    initCurrentChannelsSync(f);
                    return null;
                }, null);
            }
        };
        return openWithFileEntry(file, initAction, openAction,
                f -> clean.accept(f.path, closeSync(f)));
    }

    private void initFromDiskSync(FileEntry entry, String path, String prefix, List<String> indexPrefixes,
            boolean canDelete) {
        try {
            AsyncSegmentFile.initFromFiles(entry, path, prefix, indexPrefixes,
                    StorageUtil.listNamesSync(Paths.get(path)), canDelete);
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }

    @Override
    public List<FileChannel> closeSync(AsyncSegmentFile file) {
        return closeSyncInternal(file);
    }

    private FileEntry entryOrThrow(AsyncSegmentFile file) {
        FileEntry entry = fileEntries.get(file.key);
        if (entry == null) throw new IllegalStateException("file is closed: " + file.path);
        return entry;
    }

    @Override
    public List<FileChannel> positionSync(AsyncSegmentFile file, long offset) {
        StorageUtil.requireOpen(file);
        SegmentDirState s = entryOrThrow(file).state;
        AsyncSegmentFile.requireOffsetNotBeforeFirst(s, offset);
        file.position = offset;
        // Only the opened range has to match state; the segment channel is opened lazily on read.
        if (file.openedSegmentMatchesState(s, offset)) {
            return Collections.emptyList();
        }
        List<FileChannel> pending = new ArrayList<>();
        file.switchToSegment(offset, s, pending);
        return pending;
    }


    @Override
    public ByteBuf readSync(AsyncSegmentFile file, long length, long offset) {
        StorageUtil.requireOpen(file);
        try {
            // Open channel if needed (switch already done).
            file.openSegmentChannelForRead();
            // Read.
            long physicalOffset = offset - file.openedSegmentStartOffset;
            ByteBuf buf = readFully(file.currentSegmentChannel, length, physicalOffset, 0);
            long n = buf.readableBytes();
            maybeSwitchSegment(file, entryOrThrow(file).state, n, offset + n);
            return buf;
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }


    @Override
    public long writeSync(AsyncSegmentFile file, ByteBuf data) {
        try {
            StorageUtil.requireOpen(file);
            return writeAndFlush(file, data);
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        } finally {
            data.release();
        }
    }

    public List<FileChannel> rollMetadataSync(AsyncSegmentFile file, long currentSegmentSize, boolean noFs) {
        StorageUtil.requireOpen(file);
        FileEntry entry = entryOrThrow(file);
        return file.rollMetadata(entry, currentSegmentSize, noFs);
    }

    public void initCurrentChannelsSync(AsyncSegmentFile file) {
        StorageUtil.requireOpen(file);
        try {
            file.initCurrentChannels();
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }

    public long[] truncateSync(AsyncSegmentFile file, long offset, long endOffset, boolean noFs,
            List<FileChannel> pending) {
        StorageUtil.requireOpen(file);
        FileEntry entry = entryOrThrow(file);
        return file.truncate(offset, entry, endOffset, noFs, pending);
    }

    /**
     * Unlink the files of segments that are dropped before. Shared by truncate/delete/deleteSegments.
     *
     * mayHaveOrphanFiles is the debt marker: false means there is no unlinking work left (either
     * nothing was dropped, or a restore already ran its orphan scan). When it is set but the offsets
     * are unknown — a retry recomputes an empty drop list — a directory scan is the only way left to
     * find them.
     */
    void unlinkDroppedSegments(AsyncSegmentFile file, long[] droppedOffsets) throws IOException {
        if (!file.mayHaveOrphanFiles) {
            return;
        }
        if (droppedOffsets.length == 0) {
            file.deleteOrphanFiles(entryOrThrow(file));
        } else {
            file.deleteSegmentAndIndexFiles(droppedOffsets);
            file.mayHaveOrphanFiles = false;
        }
    }

    public void truncateLastSegmentChannel(AsyncSegmentFile file, long offset) {
        StorageUtil.requireOpen(file);
        StorageUtil.requireWriteMode(file);
        try {
            file.truncateFileAndSync(offset);
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }

    public List<FileChannel> deleteMetadataSync(AsyncSegmentFile file) {
        StorageUtil.requireOpen(file);
        FileEntry entry = entryOrThrow(file);
        return file.deleteMetadata(entry);
    }

    public long[] deleteSegmentsMetadataSync(AsyncSegmentFile file, long lastDeletedOffset) {
        StorageUtil.requireOpen(file);
        FileEntry entry = entryOrThrow(file);
        return file.deleteSegmentsMetadata(lastDeletedOffset, entry);
    }

    public void deleteSegmentsIo(AsyncSegmentFile file, long[] droppedOffsets) {
        StorageUtil.requireOpen(file);
        try {
            unlinkDroppedSegments(file, droppedOffsets);
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }

    @Override
    public SegmentDirState getSegmentDirState(AsyncSegmentFile file) {
        return entryOrThrow(file).state;
    }

    @Override
    public long getCurrentSegmentStartOffset(AsyncSegmentFile file) {
        if (file.canWrite()) {
            return file.openedSegmentStartOffset;
        }
        SegmentDirState s = entryOrThrow(file).state;
        if (s.isEmpty()) {
            return 0L;
        }
        return s.floorKey(file.position);
    }

    @Override
    public long getStartOffsetByReadOffset(AsyncSegmentFile file, long readOffset) {
        SegmentDirState s = entryOrThrow(file).state;
        if (s.isEmpty()) {
            return 0L;
        }
        return s.floorKey(readOffset);
    }


    @Override
    public Pair<Long, Map<String, AsyncIndexFile>> getCurrentIndexFilesSync(AsyncSegmentFile file, List<String> indexPrefixes,
            boolean noFs) {
        StorageUtil.requireOpen(file);
        return file.getCurrentIndexFiles(indexPrefixes, noFs);
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
    public void fsyncSync(AsyncSegmentFile file) {
        StorageUtil.requireOpen(file);
        fsyncInternal(file);
    }

    @Override
    public void deleteOrphanSegmentFilesSync(AsyncSegmentFile file) {
        StorageUtil.requireOpen(file);
        try {
            FileEntry entry = entryOrThrow(file);
            file.deleteOrphanFiles(entry);
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
    }

    @Override
    public boolean rewriteSegmentRangeSync(AsyncSegmentFile file, long startOffset, long written,
            java.util.function.LongFunction<ByteBuf> dataSupplier) {
        StorageUtil.requireOpen(file);
        return rewriteRangeSync(file.segmentPath(startOffset), startOffset, written, dataSupplier,
                file.path + " segment@" + startOffset);
    }

    @Override
    public boolean rewriteIndexRangeSync(AsyncSegmentFile file, String indexPrefix, long segmentStartOffset,
            long written, java.util.function.LongFunction<ByteBuf> dataSupplier) {
        StorageUtil.requireOpen(file);
        return rewriteRangeSync(file.pathOf(indexPrefix + segmentStartOffset), 0, written, dataSupplier,
                file.path + " " + indexPrefix + segmentStartOffset);
    }

    // baseOffset is the logical offset of file position 0 (segment start, or 0 for index files).
    // Truncate happens before dataSupplier so untrusted disk suffix is dropped even when cache
    // cannot cover the range. Returns false if dataSupplier returns null or IO fails.
    private boolean rewriteRangeSync(Path path, long baseOffset, long written,
            java.util.function.LongFunction<ByteBuf> dataSupplier, String logLabel) {
        try (FileChannel ch = FileChannel.open(path,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.READ)) {
            long diskEnd = baseOffset + ch.size();
            long logicalFrom = written <= baseOffset ? baseOffset : Math.min(written, diskEnd);
            long physicalFrom = logicalFrom - baseOffset;
            ch.truncate(physicalFrom);
            ch.position(physicalFrom);
            ByteBuf data = dataSupplier.apply(logicalFrom);
            if (data == null) {
                return false;
            }
            try {
                writeFully(ch, data);
                ch.force(true);
            } finally {
                data.release();
            }
            return true;
        } catch (Exception e) {
            logger.warn("failed to rewrite range for {} written={}", logLabel, written, e);
            return false;
        }
    }

    @Override
    public long transferToSync(AsyncFile file, long position, long count, WritableByteChannel target) {
        StorageUtil.requireOpen(file);
        try {
            if (tmpFirst(file)) {
                Pair<Long, FileChannel> tmp = openValidTmpSync(file.path);
                if (tmp.getKey() >= 0) {
                    try (FileChannel tmpCh = tmp.getValue()) {
                        return tmpCh.transferTo(TMP_HEADER_BYTES + position, count, target);
                    }
                }
            }
            return file.channel.transferTo(position, count, target);
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e, target);
        }
    }


    @Override
    public long transferToSync(AsyncSegmentFile file, long offset, long count, WritableByteChannel target) {
        StorageUtil.requireOpen(file);
        try {
            SegmentDirState s = entryOrThrow(file).state;
            file.openSegmentChannelForRead();
            long physicalOffset = offset - file.openedSegmentStartOffset;
            long n = file.currentSegmentChannel.transferTo(physicalOffset, count, target);
            maybeSwitchSegment(file, s, n, offset + n);
            return n;
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e, target);
        }
    }
}

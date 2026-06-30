package com.ctrip.xpipe.redis.keeper.storage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
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
    private final ExecutorService ioExecutor;

    public AsyncTFSBasedFileSystem(int threadCount) {
        this.ioExecutor = Executors.newFixedThreadPool(threadCount);
    }

    @Override
    public void shutdown() {
        ioExecutor.shutdown();
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
                throw new StorageIOException(e);
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
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> position(AsyncFile file, long position) {
        return CompletableFuture.runAsync(() -> {
            try {
                if (file.writeMode) {
                    throw new IllegalArgumentException("position() requires read mode");
                }
                file.channel.position(position);
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Long> read(AsyncFile file, long length, long offset, byte[] buffer) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return readFully(file.channel, length, offset, buffer);
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Long> read(AsyncFile file, long length, byte[] buffer) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return readFully(file.channel, length, buffer);
            } catch (IOException e) {
                throw new StorageIOException(e);
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
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> delete(String path) {
        return CompletableFuture.runAsync(() -> {
            try {
                Files.deleteIfExists(Paths.get(path));
            } catch (IOException e) {
                throw new StorageIOException(e);
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
                throw new StorageIOException(e);
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
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Boolean> rmdir(String path, boolean recursive) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Path dir = Paths.get(path);
                if (!Files.exists(dir)) return true;
                if (!Files.isDirectory(dir)) throw new IOException("not a directory: " + path);
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
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Boolean> truncate(AsyncFile file, long size) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                if (!file.writeMode) {
                    throw new IllegalArgumentException("truncate() requires write mode");
                }
                file.channel.truncate(size);
                if (!file.atomicReplace) {
                    file.channel.position(size);
                }
                return true;
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> close(AsyncFile file) {
        return CompletableFuture.runAsync(() -> {
            try {
                file.channel.close();
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> fsync(AsyncFile file) {
        return CompletableFuture.runAsync(() -> {
            try {
                if (!file.writeMode) {
                    throw new IllegalArgumentException("fsync() requires write mode");
                }
                file.channel.force(true);
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<List<String>> list(String path) {
        return CompletableFuture.supplyAsync(() -> {
            String[] names = new File(path).list();
            if (names == null) return Collections.emptyList();
            return Arrays.asList(names);
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
            List<IndexFileMapping> indexMappings, boolean write) {
        return CompletableFuture.supplyAsync(() -> {
            String[] names = new File(path).list();
            if (names == null) {
                throw new StorageIOException(new IOException("failed to list directory: " + path));
            }
            AsyncSegmentFile seg = new AsyncSegmentFile(path, prefix, indexMappings, write);
            seg.initFromFiles(Arrays.asList(names));
            return seg;
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> position(AsyncSegmentFile file, long offset) {
        return CompletableFuture.runAsync(() -> {
            try {
                if (file.writeMode) {
                    throw new IllegalArgumentException("position() is not supported in write mode");
                }
                file.switchToSegment(offset);
                if (file.currentSegmentChannel != null) {
                    file.currentSegmentChannel.position(offset - file.currentSegmentStartOffset);
                }
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Long> read(AsyncSegmentFile file, long length, byte[] buffer) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                if (file.currentSegmentChannel == null) {
                    if (file.segmentOffsets.isEmpty()) {
                        return 0L;
                    }
                    file.openFirstSegmentChannelForRead();
                }
                return readFully(file.currentSegmentChannel, length, buffer);
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Long> write(AsyncSegmentFile file, byte[] data, long length) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return writeFully(file.currentSegmentChannel, data, length);
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Map<String, AsyncFile>> roll(AsyncSegmentFile file) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                if (!file.writeMode) {
                    throw new IllegalArgumentException("roll() requires write mode");
                }
                return file.roll();
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public List<Long> list(AsyncSegmentFile file) {
        return new ArrayList<>(file.segmentOffsets);
    }

    @Override
    public long getCurrentSegmentStartOffset(AsyncSegmentFile file) {
        return file.currentSegmentStartOffset;
    }

    @Override
    public CompletableFuture<Map<String, AsyncFile>> getCurrentIndexFiles(AsyncSegmentFile file,
            List<String> indexPrefixes) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return file.getCurrentIndexFiles(indexPrefixes);
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Map<String, AsyncFile>> getCurrentIndexFiles(AsyncSegmentFile file) {
        List<String> prefixes = new ArrayList<>();
        for (IndexFileMapping mapping : file.indexMappings) {
            prefixes.add(mapping.prefix);
        }
        return getCurrentIndexFiles(file, prefixes);
    }

    @Override
    public CompletableFuture<Map<String, AsyncFile>> openIndexFiles(AsyncSegmentFile file, long startOffset) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                if (file.writeMode) {
                    throw new IllegalArgumentException("openIndexFiles() requires read mode");
                }
                Map<String, AsyncFile> result = new HashMap<>();
                Map<String, String> indexFileNames = file.segmentIndexFiles.get(startOffset);
                if (indexFileNames == null) return result;
                for (Map.Entry<String, String> entry : indexFileNames.entrySet()) {
                    String prefix = entry.getKey();
                    String fileName = entry.getValue();
                    String idxPath = file.absolutePathOf(fileName);
                    FileChannel ch = FileChannel.open(Paths.get(idxPath), StandardOpenOption.READ);
                    result.put(prefix, new AsyncFile(idxPath, ch, false, false));
                }
                return result;
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Long> size(AsyncSegmentFile file) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                if (file.segmentOffsets.isEmpty()) {
                    return 0L;
                }
                return file.maxValidOffset() - file.segmentOffsets.first();
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Long> lastModified(AsyncSegmentFile file) {
        if (file.segmentOffsets.isEmpty()) {
            return CompletableFuture.completedFuture(0L);
        }
        return lastModifiedOfSegment(file, file.segmentOffsets.last());
    }

    @Override
    public CompletableFuture<Long> lastModifiedOfSegment(AsyncSegmentFile file, long startOffset) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return Files.getLastModifiedTime(file.segmentPath(startOffset)).toMillis();
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Long> sizeOfSegment(AsyncSegmentFile file, long startOffset) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return Files.size(file.segmentPath(startOffset));
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> deleteSegments(AsyncSegmentFile file, List<Long> startOffsets) {
        return CompletableFuture.runAsync(() -> {
            try {
                if (!file.writeMode) {
                    throw new IllegalArgumentException("deleteSegments() requires write mode");
                }
                file.deleteSegments(startOffsets);
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Map<String, AsyncFile>> truncate(AsyncSegmentFile file, long offset) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                if (!file.writeMode) {
                    throw new IllegalArgumentException("truncate() requires write mode");
                }
                return file.truncate(offset);
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> close(AsyncSegmentFile file) {
        return CompletableFuture.runAsync(() -> {
            try {
                file.closeCurrent();
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> delete(AsyncSegmentFile file) {
        return CompletableFuture.runAsync(() -> {
            try {
                if (!file.writeMode) {
                    throw new IllegalArgumentException("delete() requires write mode");
                }
                file.delete();
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> fsync(AsyncSegmentFile file) {
        return CompletableFuture.runAsync(() -> {
            try {
                if (!file.writeMode) {
                    throw new IllegalArgumentException("fsync() requires write mode");
                }
                if (file.currentSegmentChannel != null) file.currentSegmentChannel.force(true);
            } catch (IOException e) {
                throw new StorageIOException(e);
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
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Long> transferTo(AsyncSegmentFile file, long offset, long count,
            WritableByteChannel target) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                if (offset != file.lastTransferToOffset) {
                    if (file.segmentOffsets.isEmpty()) {
                        if (offset == 0) return 0L;
                        throw new IllegalArgumentException("empty segment file, offset=" + offset);
                    }
                    long maxValid = file.maxValidOffset();
                    if (offset > maxValid) {
                        throw new IllegalArgumentException("offset " + offset + " > maxValidOffset " + maxValid);
                    }
                    if (offset == maxValid) return 0L;
                    file.switchToSegment(offset);
                }
                long physicalOffset = offset - file.currentSegmentStartOffset;
                long n = file.currentSegmentChannel.transferTo(physicalOffset, count, target);
                if (n == 0 && physicalOffset >= file.currentSegmentChannel.size()
                        && file.currentSegmentStartOffset != file.segmentOffsets.last()) {
                    file.switchToSegment(offset);
                    physicalOffset = offset - file.currentSegmentStartOffset;
                    n = file.currentSegmentChannel.transferTo(physicalOffset, count, target);
                }
                file.lastTransferToOffset = offset + n;
                return n;
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }
}

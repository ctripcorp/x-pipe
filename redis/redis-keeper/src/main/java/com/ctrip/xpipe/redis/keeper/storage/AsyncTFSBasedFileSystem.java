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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
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
                    return new AsyncFile(path, null, atomicReplace);
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
                return new AsyncFile(path, ch, atomicReplace);
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
                file.channel.position(position);
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Integer> read(AsyncFile file, long length, long offset, byte[] buffer) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return readFully(file.channel, length, offset, buffer);
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Integer> read(AsyncFile file, long length, byte[] buffer) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return readFully(file.channel, length, buffer);
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    // TODO how to handle the race condition?
    @Override
    public CompletableFuture<Integer> write(AsyncFile file, byte[] data, long length) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                if (file.atomicReplace) {
                    return atomicReplaceWrite(file, data, (int) Math.min(length, data.length));
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
                file.channel.truncate(size);
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

    private int readFully(FileChannel ch, long length, byte[] buffer) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(buffer, 0, (int) Math.min(length, buffer.length));
        int totalRead = 0;
        while (buf.hasRemaining()) {
            int n = ch.read(buf);
            if (n < 0) break;
            totalRead += n;
        }
        return totalRead;
    }

    private int readFully(FileChannel ch, long length, long offset, byte[] buffer) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(buffer, 0, (int) Math.min(length, buffer.length));
        int totalRead = 0;
        while (buf.hasRemaining()) {
            int n = ch.read(buf, offset + totalRead);
            if (n < 0) break;
            totalRead += n;
        }
        return totalRead;
    }

    private int writeFully(FileChannel ch, byte[] data, long length) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(data, 0, (int) Math.min(length, data.length));
        int totalWritten = 0;
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
            int lenRead = readFully(tmpCh, 8, lenBytes);
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
            int dataRead = readFully(tmpCh, expectedLen, dataBytes);
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

    private int atomicReplaceWrite(AsyncFile file, byte[] data, int length) throws IOException {
        Path tmpPath = getTmpPath(file.path);
        try (FileChannel tmpCh = FileChannel.open(tmpPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer lenBuf = ByteBuffer.allocate(8);
            lenBuf.putLong(length);
            lenBuf.flip();
            writeFully(tmpCh, lenBuf.array(), 8);
            writeFully(tmpCh, data, length);
            tmpCh.force(true);
        }
        file.channel.truncate(0);
        file.channel.position(0);
        int written = writeFully(file.channel, data, length);
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
                ensureReadSegment(file, offset);
                file.currentSegmentChannel.position(offset - file.currentSegmentStartOffset);
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Integer> read(AsyncSegmentFile file, long length, byte[] buffer) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                long logicalPos = file.currentSegmentChannel == null
                        ? 0 : file.currentSegmentStartOffset + file.currentSegmentChannel.position();
                ensureReadSegment(file, logicalPos);
                file.currentSegmentChannel.position(logicalPos - file.currentSegmentStartOffset);
                return readFully(file.currentSegmentChannel, length, buffer);
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Integer> write(AsyncSegmentFile file, byte[] data, long length) {
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
                long newStart = file.currentSegmentStartOffset + file.currentSegmentChannel.size();
                if (file.currentSegmentChannel != null) file.currentSegmentChannel.close();
                file.currentSegmentStartOffset = newStart;
                file.currentSegmentChannel = FileChannel.open(
                        Paths.get(segmentPath(file.dirPath, file.prefix, file.currentSegmentStartOffset)),
                        StandardOpenOption.WRITE, StandardOpenOption.CREATE);
                file.currentSegmentChannel.position(file.currentSegmentChannel.size());
                return openIndexFileMap(file.dirPath, file.indexMappings, file.currentSegmentStartOffset);
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public List<Long> list(AsyncSegmentFile file) {
        return listSegmentOffsets(file.dirPath, file.prefix);
    }

    @Override
    public long getCurrentSegmentStartOffset(AsyncSegmentFile file) {
        return file.currentSegmentStartOffset;
    }

    @Override
    public CompletableFuture<Map<String, AsyncFile>> getCurrentIndexFiles(AsyncSegmentFile file,
            List<IndexFileMapping> indexMappings) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return openIndexFileMap(file.dirPath, indexMappings, file.currentSegmentStartOffset);
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Map<String, AsyncFile>> getCurrentIndexFiles(AsyncSegmentFile file) {
        return getCurrentIndexFiles(file, file.indexMappings);
    }

    @Override
    public CompletableFuture<Map<String, AsyncFile>> openIndexFiles(AsyncSegmentFile file, long startOffset) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return openIndexFileMap(file.dirPath, file.indexMappings, startOffset);
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Long> size(AsyncSegmentFile file) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return logicalSize(file);
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Long> lastModified(AsyncSegmentFile file) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return lastModifiedOfSegment(file, file.currentSegmentStartOffset).get();
            } catch (IOException e) {
                throw new StorageIOException(e);
            } catch (Exception e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Long> lastModifiedOfSegment(AsyncSegmentFile file, long startOffset) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return Files.getLastModifiedTime(Paths.get(segmentPath(file.dirPath, file.prefix, startOffset))).toMillis();
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Long> sizeOfSegment(AsyncSegmentFile file, long startOffset) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return Files.size(Paths.get(segmentPath(file.dirPath, file.prefix, startOffset)));
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> deleteSegments(AsyncSegmentFile file, List<Long> startOffsets) {
        return CompletableFuture.runAsync(() -> {
            try {
                for (long offset : startOffsets) {
                    Files.deleteIfExists(Paths.get(segmentPath(file.dirPath, file.prefix, offset)));
                    for (IndexFileMapping mapping : file.indexMappings) {
                        Files.deleteIfExists(Paths.get(file.dirPath, mapping.offsetToFileName.apply(offset)));
                    }
                }
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> truncate(AsyncSegmentFile file, long offset) {
        return CompletableFuture.runAsync(() -> {
            try {
                List<Long> offsets = listSegmentOffsets(file.dirPath, file.prefix);
                long targetStart = -1;
                for (int i = offsets.size() - 1; i >= 0; i--) {
                    if (offsets.get(i) <= offset) {
                        targetStart = offsets.get(i);
                        break;
                    }
                }
                if (targetStart < 0) return;

                String targetPath = segmentPath(file.dirPath, file.prefix, targetStart);
                try (FileChannel ch = FileChannel.open(Paths.get(targetPath), StandardOpenOption.WRITE)) {
                    ch.truncate(offset - targetStart);
                }

                for (long o : offsets) {
                    if (o > targetStart) {
                        Files.deleteIfExists(Paths.get(segmentPath(file.dirPath, file.prefix, o)));
                        for (IndexFileMapping mapping : file.indexMappings) {
                            Files.deleteIfExists(Paths.get(file.dirPath, mapping.offsetToFileName.apply(o)));
                        }
                    }
                }

                if (file.writeMode && file.currentSegmentStartOffset > targetStart) {
                    if (file.currentSegmentChannel != null) file.currentSegmentChannel.close();
                    file.currentSegmentStartOffset = targetStart;
                    file.currentSegmentChannel = FileChannel.open(Paths.get(targetPath),
                            StandardOpenOption.WRITE, StandardOpenOption.CREATE);
                    file.currentSegmentChannel.position(file.currentSegmentChannel.size());
                }
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> close(AsyncSegmentFile file) {
        return CompletableFuture.runAsync(() -> {
            try {
                if (file.currentSegmentChannel != null) file.currentSegmentChannel.close();
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> fsync(AsyncSegmentFile file) {
        return CompletableFuture.runAsync(() -> {
            try {
                if (file.currentSegmentChannel != null) file.currentSegmentChannel.force(true);
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    // Non-blocking: single transferTo attempt; caller must retry if fewer bytes were transferred.
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

    // Non-blocking: single transferTo attempt per segment; caller must retry if fewer bytes were transferred.
    @Override
    public CompletableFuture<Long> transferTo(AsyncSegmentFile file, long offset, long count,
            WritableByteChannel target) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                ensureReadSegment(file, offset);
                long physicalOffset = offset - file.currentSegmentStartOffset;
                long segmentRemaining = file.currentSegmentChannel.size() - physicalOffset;
                long toTransfer = Math.min(count, segmentRemaining);
                if (toTransfer <= 0) return 0L;
                return file.currentSegmentChannel.transferTo(physicalOffset, toTransfer, target);
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    // ---- helpers ----

    private String segmentPath(String dir, String prefix, long startOffset) {
        return dir + File.separator + prefix + startOffset;
    }

    private List<Long> listSegmentOffsets(String dir, String prefix) {
        File[] files = new File(dir).listFiles((d, name) -> name.startsWith(prefix));
        if (files == null) return Collections.emptyList();
        return Arrays.stream(files)
                .map(f -> Long.parseLong(f.getName().substring(prefix.length())))
                .sorted()
                .collect(Collectors.toList());
    }
    private void ensureReadSegment(AsyncSegmentFile file, long logicalOffset) throws IOException {
        List<Long> offsets = listSegmentOffsets(file.dirPath, file.prefix);
        long segStart = -1;
        for (int i = offsets.size() - 1; i >= 0; i--) {
            if (offsets.get(i) <= logicalOffset) {
                segStart = offsets.get(i);
                break;
            }
        }
        if (segStart < 0) throw new IOException("no segment for offset " + logicalOffset);
        if (segStart != file.currentSegmentStartOffset) {
            if (file.currentSegmentChannel != null) file.currentSegmentChannel.close();
            file.currentSegmentStartOffset = segStart;
            file.currentSegmentChannel = FileChannel.open(
                    Paths.get(segmentPath(file.dirPath, file.prefix, segStart)), StandardOpenOption.READ);
        }
    }

    private long logicalSize(AsyncSegmentFile file) throws IOException {
        List<Long> offsets = listSegmentOffsets(file.dirPath, file.prefix);
        if (offsets.isEmpty()) return 0;
        long lastOffset = offsets.get(offsets.size() - 1);
        return lastOffset + Files.size(Paths.get(segmentPath(file.dirPath, file.prefix, lastOffset)));
    }

    private Map<String, AsyncFile> openIndexFileMap(String dir, List<IndexFileMapping> indexMappings,
            long startOffset) throws IOException {
        Map<String, AsyncFile> result = new LinkedHashMap<>();
        for (IndexFileMapping mapping : indexMappings) {
            String fileName = mapping.offsetToFileName.apply(startOffset);
            String idxPath = dir + File.separator + fileName;
            FileChannel ch = FileChannel.open(Paths.get(idxPath), StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            ch.position(ch.size());
            result.put(mapping.prefix, new AsyncFile(idxPath, ch));
        }
        return result;
    }
}

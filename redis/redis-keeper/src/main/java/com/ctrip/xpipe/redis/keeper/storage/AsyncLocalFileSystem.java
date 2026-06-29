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

public class AsyncLocalFileSystem implements AsyncFileSystem {

    private final ExecutorService ioExecutor;

    public AsyncLocalFileSystem(int threadCount) {
        this.ioExecutor = Executors.newFixedThreadPool(threadCount);
    }

    @Override
    public void shutdown() {
        ioExecutor.shutdown();
    }

    // ---- AsyncFile ----

    @Override
    public CompletableFuture<AsyncFile> open(String path, boolean write, boolean atomicReplace, boolean allowDirectory) {
        if (atomicReplace && !write) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("atomicReplace requires write=true"));
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                Path p = Paths.get(path);
                if (Files.isDirectory(p)) {
                    if (allowDirectory) {
                        return new AsyncFile(path, null, atomicReplace, true);
                    } else {
                        throw new IllegalArgumentException("path is a directory: " + path);
                    }
                }
                FileChannel ch;
                if (!write) {
                    ch = FileChannel.open(p, StandardOpenOption.READ);
                } else {
                    ch = FileChannel.open(p, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
                    if (!atomicReplace) ch.position(ch.size());
                }
                return new AsyncFile(path, ch, atomicReplace, false);
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
                ByteBuffer buf = ByteBuffer.wrap(buffer, 0, (int) Math.min(length, buffer.length));
                return file.channel.read(buf, offset);
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Integer> read(AsyncFile file, long length, byte[] buffer) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                ByteBuffer buf = ByteBuffer.wrap(buffer, 0, (int) Math.min(length, buffer.length));
                return file.channel.read(buf);
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Integer> write(AsyncFile file, byte[] data, long length) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                int len = (int) Math.min(length, data.length);
                ByteBuffer buf = ByteBuffer.wrap(data, 0, len);
                // TODO: should we use a temporary file to avoid the race condition?
                if (file.atomicReplace) {
                    file.channel.truncate(0);
                    while (buf.hasRemaining()) {
                        file.channel.write(buf);
                    }
                    return len;
                }
                return file.channel.write(buf);
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
                ByteBuffer buf = ByteBuffer.wrap(buffer, 0, (int) Math.min(length, buffer.length));
                return file.currentSegmentChannel.read(buf);
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Integer> write(AsyncSegmentFile file, byte[] data, long length) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                ByteBuffer buf = ByteBuffer.wrap(data, 0, (int) Math.min(length, data.length));
                int n = file.currentSegmentChannel.write(buf);
                return n;
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

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
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class AsyncLocalFileSystem implements AsyncFileSystem {

    private final ExecutorService ioExecutor;

    public AsyncLocalFileSystem(int threadCount) {
        this.ioExecutor = Executors.newFixedThreadPool(threadCount);
    }

    // ---- AsyncFile ----

    @Override
    public CompletableFuture<AsyncFile> open(String path, boolean write, boolean atomicReplace) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Set<StandardOpenOption> opts = atomicReplace
                        ? EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE)
                        : toOpenOptions(write);
                FileChannel ch = FileChannel.open(Paths.get(path), opts);
                return new AsyncFile(path, ch, atomicReplace);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Boolean> isFile(AsyncFile file) {
        return CompletableFuture.supplyAsync(
                () -> Files.isRegularFile(Paths.get(file.path)), ioExecutor);
    }

    @Override
    public CompletableFuture<Long> lastModified(AsyncFile file) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return Files.getLastModifiedTime(Paths.get(file.path)).toMillis();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, ioExecutor);
    }

    @Override
    public void position(AsyncFile file, long position) {
        try {
            file.channel.position(position);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Integer> read(AsyncFile file, long length, long offset, byte[] buffer) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                ByteBuffer buf = ByteBuffer.wrap(buffer, 0, (int) Math.min(length, buffer.length));
                return file.channel.read(buf, offset);
            } catch (IOException e) {
                throw new RuntimeException(e);
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
                throw new RuntimeException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Integer> write(AsyncFile file, byte[] data, long length) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                int len = (int) Math.min(length, data.length);
                ByteBuffer buf = ByteBuffer.wrap(data, 0, len);
                if (file.atomicReplace) {
                    file.channel.truncate(0);
                    while (buf.hasRemaining()) {
                        file.channel.write(buf);
                    }
                    return len;
                }
                return file.channel.write(buf);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> delete(String path) {
        return CompletableFuture.runAsync(() -> {
            try {
                Files.deleteIfExists(Paths.get(path));
            } catch (IOException e) {
                throw new RuntimeException(e);
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
                throw new RuntimeException(e);
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
                    Files.createDirectory(Paths.get(path));
                }
                return true;
            } catch (FileAlreadyExistsException e) {
                return false;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Boolean> rmdir(String path, boolean recursive) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Path dir = Paths.get(path);
                if (!Files.exists(dir)) return false;
                if (recursive) {
                    Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                            Files.delete(file);
                            return FileVisitResult.CONTINUE;
                        }
                        @Override
                        public FileVisitResult postVisitDirectory(Path d, IOException exc) throws IOException {
                            Files.delete(d);
                            return FileVisitResult.CONTINUE;
                        }
                    });
                } else {
                    Files.delete(dir);
                }
                return true;
            } catch (IOException e) {
                throw new RuntimeException(e);
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
                throw new RuntimeException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> close(AsyncFile file) {
        return CompletableFuture.runAsync(() -> {
            try {
                file.channel.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> fsync(AsyncFile file) {
        return CompletableFuture.runAsync(() -> {
            try {
                file.channel.force(true);
            } catch (IOException e) {
                throw new RuntimeException(e);
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
            List<String> indexPrefixes, boolean write) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                AsyncSegmentFile seg = new AsyncSegmentFile(path, prefix, indexPrefixes, write);
                if (write) {
                    List<Long> offsets = listSegmentOffsets(path, prefix);
                    long startOffset = offsets.isEmpty() ? 0L : offsets.get(offsets.size() - 1);
                    seg.currentSegmentStartOffset = startOffset;
                    seg.channel = openChannel(segmentPath(path, prefix, startOffset), true);
                    seg.logicalPosition = startOffset + seg.channel.size();
                }
                return seg;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, ioExecutor);
    }

    // Synchronous: just updates the logical position field.
    @Override
    public void position(AsyncSegmentFile file, long offset) {
        file.logicalPosition = offset;
    }

    @Override
    public CompletableFuture<Integer> read(AsyncSegmentFile file, long length, long offset, byte[] buffer) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                ensureReadSegment(file, offset);
                ByteBuffer buf = ByteBuffer.wrap(buffer, 0, (int) Math.min(length, buffer.length));
                return file.channel.read(buf, offset - file.currentSegmentStartOffset);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Integer> read(AsyncSegmentFile file, long length, byte[] buffer) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                ensureReadSegment(file, file.logicalPosition);
                ByteBuffer buf = ByteBuffer.wrap(buffer, 0, (int) Math.min(length, buffer.length));
                int n = file.channel.read(buf, file.logicalPosition - file.currentSegmentStartOffset);
                if (n > 0) file.logicalPosition += n;
                return n;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Integer> write(AsyncSegmentFile file, byte[] data, long length) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                ByteBuffer buf = ByteBuffer.wrap(data, 0, (int) Math.min(length, data.length));
                int n = file.channel.write(buf);
                file.logicalPosition += n;
                return n;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, ioExecutor);
    }

    // Synchronous: caller must not interleave with in-flight writes.
    @Override
    public void roll(AsyncSegmentFile file) {
        try {
            if (file.channel != null) file.channel.close();
            file.currentSegmentStartOffset = file.logicalPosition;
            file.channel = openChannel(
                    segmentPath(file.dirPath, file.prefix, file.currentSegmentStartOffset),
                    true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Long> list(AsyncSegmentFile file) {
        return listSegmentOffsets(file.dirPath, file.prefix);
    }

    @Override
    public CompletableFuture<Map<String, AsyncFile>> getCurrentIndexFiles(AsyncSegmentFile file,
            List<String> indexPrefixes) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return openIndexFileMap(file.dirPath, indexPrefixes, file.currentSegmentStartOffset);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Map<String, AsyncFile>> getCurrentIndexFiles(AsyncSegmentFile file) {
        return getCurrentIndexFiles(file, file.indexPrefixes);
    }

    @Override
    public Map<String, AsyncFile> openIndexFiles(AsyncSegmentFile file, long startOffset) {
        try {
            return openIndexFileMap(file.dirPath, file.indexPrefixes, startOffset);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Long> size(AsyncSegmentFile file) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return logicalSize(file);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Long> sizeOfSegment(AsyncSegmentFile file, long startOffset) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return Files.size(Paths.get(segmentPath(file.dirPath, file.prefix, startOffset)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> deleteSegments(AsyncSegmentFile file, List<Long> startOffsets) {
        return CompletableFuture.runAsync(() -> {
            try {
                for (long offset : startOffsets) {
                    Files.deleteIfExists(Paths.get(segmentPath(file.dirPath, file.prefix, offset)));
                    for (String idxPrefix : file.indexPrefixes) {
                        Files.deleteIfExists(Paths.get(segmentPath(file.dirPath, idxPrefix, offset)));
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
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
                        for (String idxPrefix : file.indexPrefixes) {
                            Files.deleteIfExists(Paths.get(segmentPath(file.dirPath, idxPrefix, o)));
                        }
                    }
                }

                if (file.writeMode && file.currentSegmentStartOffset > targetStart) {
                    if (file.channel != null) file.channel.close();
                    file.currentSegmentStartOffset = targetStart;
                    file.channel = openChannel(targetPath, true);
                }
                file.logicalPosition = offset;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> close(AsyncSegmentFile file) {
        return CompletableFuture.runAsync(() -> {
            try {
                if (file.channel != null) file.channel.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, ioExecutor);
    }

    @Override
    public CompletableFuture<Void> fsync(AsyncSegmentFile file) {
        return CompletableFuture.runAsync(() -> {
            try {
                if (file.channel != null) file.channel.force(true);
            } catch (IOException e) {
                throw new RuntimeException(e);
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
                throw new RuntimeException(e);
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
                long segmentRemaining = file.channel.size() - physicalOffset;
                long toTransfer = Math.min(count, segmentRemaining);
                if (toTransfer <= 0) return 0L;
                return file.channel.transferTo(physicalOffset, toTransfer, target);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, ioExecutor);
    }

    // ---- helpers ----

    private Set<StandardOpenOption> toOpenOptions(boolean write) {
        if (write) {
            return EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } else {
            return EnumSet.of(StandardOpenOption.READ);
        }
    }

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

    private FileChannel openChannel(String path, boolean write) throws IOException {
        return FileChannel.open(Paths.get(path), toOpenOptions(write));
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
            if (file.channel != null) file.channel.close();
            file.currentSegmentStartOffset = segStart;
            file.channel = openChannel(segmentPath(file.dirPath, file.prefix, segStart), false);
        }
    }

    private long logicalSize(AsyncSegmentFile file) throws IOException {
        List<Long> offsets = listSegmentOffsets(file.dirPath, file.prefix);
        if (offsets.isEmpty()) return 0;
        long lastOffset = offsets.get(offsets.size() - 1);
        return lastOffset + Files.size(Paths.get(segmentPath(file.dirPath, file.prefix, lastOffset)));
    }

    private Map<String, AsyncFile> openIndexFileMap(String dir, List<String> indexPrefixes,
            long startOffset) throws IOException {
        Map<String, AsyncFile> result = new LinkedHashMap<>();
        for (String prefix : indexPrefixes) {
            String idxPath = segmentPath(dir, prefix, startOffset);
            result.put(prefix, new AsyncFile(idxPath, openChannel(idxPath, true)));
        }
        return result;
    }
}

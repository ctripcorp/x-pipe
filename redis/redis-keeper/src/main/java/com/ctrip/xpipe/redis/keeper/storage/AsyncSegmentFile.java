package com.ctrip.xpipe.redis.keeper.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AsyncSegmentFile extends AbstractStorageFile {

    private static final Logger logger = LoggerFactory.getLogger(AsyncSegmentFile.class);

    final String dirPath;
    final String prefix;
    final List<String> indexPrefixes;
    final String key;

    FileChannel currentSegmentChannel;
    Map<String, AsyncIndexFile> currentIndexFiles;
    // Currently open segment covers logical range [openedSegmentStartOffset, openedSegmentEndOffset).
    // Long.MAX_VALUE for start means "no segment currently open"; Long.MAX_VALUE for end
    // means "open segment is the tail — end is unbounded (writer may still append)".
    long openedSegmentStartOffset = Long.MAX_VALUE;
    long openedSegmentEndOffset = Long.MAX_VALUE;
    long position = 0;

    @Override
    FileChannel currentWriteChannel() {
        return currentSegmentChannel;
    }

    @Override
    void openCurrentChannel() throws IOException {
        if (canWrite()) {
            currentSegmentChannel = FileChannel.open(segmentPath(openedSegmentStartOffset),
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            currentSegmentChannel.position(currentSegmentChannel.size());
        } else {
            currentSegmentChannel = FileChannel.open(segmentPath(openedSegmentStartOffset),
                    StandardOpenOption.READ, StandardOpenOption.CREATE);
        }
    }

    @Override
    void reopenCurrentChannel() throws IOException {
        if (currentSegmentChannel == null) {
            return;
        }
        currentSegmentChannel.close();
        try {
            openCurrentChannel();
        } catch (IOException e) {
            currentSegmentChannel = null;
            throw e;
        }
    }

    @Override
    String getKey() {
        return key;
    }

    @Override
    String identifier() {
        return key;
    }

    @Override
    SegmentFileCacheEntry getCacheEntry() {
        return (SegmentFileCacheEntry) cacheEntry;
    }

    void setCacheEntry(SegmentFileCacheEntry entry) {
        this.cacheEntry = entry;
        for (AsyncIndexFile af : currentIndexFiles.values()) {
            if (af.cacheEntry == null) {
                bindIndexFileCacheEntry(af);
            }
        }
    }

    private void bindIndexFileCacheEntry(AsyncIndexFile af) {
        SegmentFileCacheEntry segmentEntry = getCacheEntry();
        if (segmentEntry == null) {
            return;
        }
        boolean write = af.canWrite();
        af.cacheEntry = segmentEntry.acquireIndexFileCacheEntry(af.startOffset, af.indexPrefix, write);
        af.onCacheClose = () -> segmentEntry.releaseIndexFileCacheEntry(af.startOffset, af.indexPrefix, write);
    }

    private AsyncIndexFile openIndexFile(String indexPrefix, long startOffset, OpenMode mode) throws IOException {
        String fileName = indexPrefix + startOffset;
        AsyncIndexFile af = new AsyncIndexFile(key, absolutePathOf(fileName), indexPrefix, startOffset, mode);
        bindIndexFileCacheEntry(af);
        af.openCurrentChannel();
        return af;
    }

    AsyncSegmentFile(String dirPath, String prefix, List<String> indexPrefixes, String key, boolean writeMode) {
        super(writeMode ? OpenMode.WRITE : OpenMode.READ);
        this.dirPath = dirPath;
        this.prefix = prefix;
        this.indexPrefixes = indexPrefixes;
        this.key = key;
        this.currentIndexFiles = new HashMap<>();
    }

    // Called once by the initializer opener.
    // Scans the directory, builds the maximal contiguous segment chain, deletes invalid files,
    // and publishes the initial SegmentDirState into entry.
    static void initFromFiles(FileEntry entry, String dirPath, String prefix, List<String> indexPrefixes,
            List<String> allFiles) throws IOException {
        List<long[]> segs = new ArrayList<>();
        Map<Long, List<String>> indexCandidates = new HashMap<>();

        for (String name : allFiles) {
            if (name.startsWith(prefix)) {
                try {
                    long offset = Long.parseLong(name.substring(prefix.length()));
                    long size = Files.size(Paths.get(dirPath, name));
                    segs.add(new long[]{offset, size});
                } catch (NumberFormatException e) {
                    logger.warn("Deleting unrecognized file in {}: {}", dirPath, name);
                    Files.deleteIfExists(Paths.get(dirPath, name));
                }
            } else {
                for (String indexPrefix : indexPrefixes) {
                    if (!name.startsWith(indexPrefix)) continue;
                    try {
                        long offset = Long.parseLong(name.substring(indexPrefix.length()));
                        indexCandidates
                                .computeIfAbsent(offset, k -> new ArrayList<>())
                                .add(name);
                    } catch (NumberFormatException e) {
                        logger.warn("Deleting unrecognized file in {}: {}", dirPath, name);
                        Files.deleteIfExists(Paths.get(dirPath, name));
                    }
                    break;
                }
            }
        }

        segs.sort((a, b) -> Long.compare(b[0], a[0]));

        Set<Long> validOffsets = new HashSet<>();
        if (!segs.isEmpty()) {
            validOffsets.add(segs.get(0)[0]);
            long chainHead = segs.get(0)[0];
            for (int i = 1; i < segs.size(); i++) {
                long[] seg = segs.get(i);
                long segEnd = seg[0] + seg[1];
                if (segEnd == chainHead) {
                    validOffsets.add(seg[0]);
                    chainHead = seg[0];
                } else {
                    if (segEnd > chainHead) {
                        logger.warn("Overlapping segment in {}: {} ends at {} but chain head is {}",
                                dirPath, prefix + seg[0], segEnd, chainHead);
                    }
                    logger.warn("Deleting off-chain segment in {}: {}", dirPath, prefix + seg[0]);
                    Files.deleteIfExists(Paths.get(dirPath, prefix + seg[0]));
                }
            }
        }

        for (Map.Entry<Long, List<String>> byOffset : indexCandidates.entrySet()) {
            if (!validOffsets.contains(byOffset.getKey())) {
                List<String> files = byOffset.getValue();
                logger.warn("Deleting off-chain index files in {}: {}", dirPath, files);
                for (String name : files) {
                    Files.deleteIfExists(Paths.get(dirPath, name));
                }
            }
        }

        if (validOffsets.isEmpty()) {
            entry.state = SegmentDirState.EMPTY;
        } else {
            long[] arr = new long[validOffsets.size()];
            int i = 0;
            for (long o : validOffsets) arr[i++] = o;
            Arrays.sort(arr);
            entry.state = new SegmentDirState(arr);
        }
    }

    // Called after initFromFiles has populated the shared state.
    // Opens resources: for writer, open the tail segment + index files for append;
    // for reader, seek position to the first segment.
    void openInitialResources(SegmentDirState s) throws IOException {
        if (s.isEmpty()) return;

        if (canWrite()) {
            openedSegmentStartOffset = s.lastOffset;
            openedSegmentEndOffset = Long.MAX_VALUE;
            openCurrentChannel();
            currentIndexFiles = new HashMap<>();
            for (String indexPrefix : indexPrefixes) {
                AsyncIndexFile af = openIndexFile(indexPrefix, openedSegmentStartOffset, OpenMode.READ_WRITE);
                currentIndexFiles.put(indexPrefix, af);
            }
        } else {
            position = s.firstOffset;
        }
    }

    // ---- path helpers ----

    Path segmentPath(long offset) {
        return Paths.get(dirPath, prefix + offset);
    }

    Path pathOf(String fileName) {
        return Paths.get(dirPath, fileName);
    }

    String segmentAbsolutePath(long offset) {
        return dirPath + File.separator + prefix + offset;
    }

    String absolutePathOf(String fileName) {
        return dirPath + File.separator + fileName;
    }

    // ---- io / state helpers ----

    void closeCurrent() throws IOException {
        IOException first = null;
        try {
            if (currentSegmentChannel != null) currentSegmentChannel.close();
        } catch (IOException e) {
            first = e;
        } finally {
            currentSegmentChannel = null;
        }
        for (AsyncIndexFile af : currentIndexFiles.values()) {
            try {
                af.channel.close();
            } catch (IOException e) {
                if (first == null) first = e;
            } finally {
                try {
                    af.onCacheClose.run();
                } catch (Throwable t) {
                    logger.error("onCacheClose failed for {}", af.identifier(), t);
                }
            }
        }
        currentIndexFiles.clear();
        openedSegmentStartOffset = Long.MAX_VALUE;
        openedSegmentEndOffset = Long.MAX_VALUE;
        pendingFsyncBytes = 0;
        if (first != null) throw first;
    }

    long exclusiveEndOffset(long lastOffset) throws IOException {
        return lastOffset + Files.size(segmentPath(lastOffset));
    }

    private void deleteSegmentAndIndex(long offset) throws IOException {
        Files.deleteIfExists(segmentPath(offset));
        for (String indexPrefix : indexPrefixes) {
            Files.deleteIfExists(pathOf(indexPrefix + offset));
        }
    }

    private Map<String, AsyncFile> createNewSegmentWithIndexes(long startOffset, FileEntry entry) throws IOException {
        openedSegmentStartOffset = startOffset;
        openedSegmentEndOffset = Long.MAX_VALUE;
        openCurrentChannel();

        Map<String, AsyncFile> result = new HashMap<>();
        for (String indexPrefix : indexPrefixes) {
            AsyncIndexFile af = openIndexFile(indexPrefix, startOffset, OpenMode.READ_WRITE);
            currentIndexFiles.put(indexPrefix, af);
            result.put(indexPrefix, af);
        }

        entry.state = new SegmentDirState(entry.state.copyAppend(startOffset));
        return result;
    }

    boolean switchToSegment(long logicalOffset, SegmentDirState s) throws IOException {
        if (logicalOffset >= openedSegmentStartOffset && logicalOffset < openedSegmentEndOffset) {
            return true;
        }
        if (s.isEmpty()) {
            closeCurrent();
            return false;
        }
        if (logicalOffset < s.firstOffset) {
            closeCurrent();
            return false;
        }
        if (logicalOffset >= exclusiveEndOffset(s.lastOffset)) {
            closeCurrent();
            return false;
        }
        long segStart = s.floorKey(logicalOffset);
        closeCurrent();
        openedSegmentStartOffset = segStart;
        if (segStart == s.lastOffset) {
            openedSegmentEndOffset = Long.MAX_VALUE;
        } else {
            openedSegmentEndOffset = segStart + Files.size(segmentPath(segStart));
        }
        return true;
    }

    void openSegmentChannelForRead() throws IOException {
        if (currentSegmentChannel != null) {
            return;
        }
        openCurrentChannel();
    }

    // lazy-create the first segment (starting at offset 0) and its index files for write mode.
    void openFirstSegmentChannelForWrite(FileEntry entry) throws IOException {
        createNewSegmentWithIndexes(0, entry);
    }

    Map<String, AsyncFile> getCurrentIndexFiles(List<String> requestedPrefixes) throws IOException {
        Map<String, AsyncFile> result = new HashMap<>();
        for (String indexPrefix : requestedPrefixes) {
            AsyncIndexFile af = currentIndexFiles.get(indexPrefix);
            if (af != null) {
                result.put(indexPrefix, af);
            } else if (canWrite()) {
                logger.error("Index channel for prefix {} is null in write mode, segment offset: {}",
                        indexPrefix, openedSegmentStartOffset);
                throw new IllegalStateException("Index channel for prefix " + indexPrefix + " is null in write mode");
            } else {
                String fileName = indexPrefix + openedSegmentStartOffset;
                Path p = pathOf(fileName);
                if (Files.exists(p)) {
                    af = openIndexFile(indexPrefix, openedSegmentStartOffset, OpenMode.READ);
                    currentIndexFiles.put(indexPrefix, af);
                    result.put(indexPrefix, af);
                }
            }
        }
        return result;
    }

    void deleteSegments(List<Long> startOffsets, FileEntry entry) throws IOException {
        SegmentDirState cur = entry.state;
        int drop = 0;
        for (long offset : startOffsets) {
            long first = cur.get(drop);
            if (offset != first) {
                throw new IllegalArgumentException("deleteSegments requires deleting segments in order from the first: expected " + first + ", got " + offset);
            }
            if (cur.size() - drop <= 1) {
                throw new IllegalArgumentException("deleteSegments cannot delete the last segment");
            }
            drop++;
        }
        entry.state = new SegmentDirState(cur.copyFrom(drop));
        for (int i = 0; i < drop; i++) {
            deleteSegmentAndIndex(cur.get(i));
        }
    }

    void delete(FileEntry entry) throws IOException {
        SegmentDirState cur = entry.state;
        entry.state = SegmentDirState.EMPTY;
        closeCurrent();
        for (int i = 0; i < cur.size(); i++) {
            deleteSegmentAndIndex(cur.get(i));
        }
    }

    Map<String, AsyncFile> truncate(long offset, FileEntry entry) throws IOException {
        SegmentDirState s = entry.state;
        if (!s.isEmpty() && offset >= s.firstOffset && offset <= exclusiveEndOffset(s.lastOffset)) {
            return truncateInRange(offset, entry);
        }
        return reset(offset, entry);
    }

    private Map<String, AsyncFile> truncateInRange(long offset, FileEntry entry) throws IOException {
        SegmentDirState cur = entry.state;
        long targetStart = cur.floorKey(offset);
        boolean reuseCurrent = openedSegmentStartOffset == targetStart;

        if (!reuseCurrent) {
            closeCurrent();
            openedSegmentStartOffset = targetStart;
            openCurrentChannel();
        }
        openedSegmentEndOffset = Long.MAX_VALUE;

        int cut = cur.indexOf(targetStart) + 1;
        long[] nextArr = cur.copyShrink(cut);

        long newSegmentSize = offset - targetStart;
        long oldSegmentSize = currentSegmentChannel.size();
        currentSegmentChannel.truncate(newSegmentSize);
        currentSegmentChannel.position(newSegmentSize);
        if (newSegmentSize < oldSegmentSize) {
            pendingFsyncBytes = Math.max(0, pendingFsyncBytes - (oldSegmentSize - newSegmentSize));
        }

        Map<String, AsyncFile> result = new HashMap<>();
        for (String indexPrefix : indexPrefixes) {
            AsyncIndexFile af = currentIndexFiles.get(indexPrefix);
            if (af == null) {
                af = openIndexFile(indexPrefix, targetStart, OpenMode.READ_WRITE);
                currentIndexFiles.put(indexPrefix, af);
            }
            result.put(indexPrefix, af);
        }

        entry.state = new SegmentDirState(nextArr);
        for (int i = cut; i < cur.size(); i++) {
            deleteSegmentAndIndex(cur.get(i));
        }
        return result;
    }

    private Map<String, AsyncFile> reset(long offset, FileEntry entry) throws IOException {
        closeCurrent();
        SegmentDirState cur = entry.state;
        entry.state = SegmentDirState.EMPTY;
        for (int i = 0; i < cur.size(); i++) {
            deleteSegmentAndIndex(cur.get(i));
        }
        return createNewSegmentWithIndexes(offset, entry);
    }

    Map<String, AsyncFile> roll(FileEntry entry) throws IOException {
        long newStartOffset = openedSegmentStartOffset + currentSegmentChannel.size();
        closeCurrent();
        return createNewSegmentWithIndexes(newStartOffset, entry);
    }
}

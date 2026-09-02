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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ctrip.xpipe.tuple.Pair;

public class AsyncSegmentFile extends AbstractStorageFile {

    private static final Logger logger = LoggerFactory.getLogger(AsyncSegmentFile.class);

    static void requireOffsetNotBeforeFirst(SegmentDirState s, long logicalOffset) {
        if (!s.isEmpty() && logicalOffset < s.firstOffset) {
            throw new SegmentOffsetBeforeFirstException(
                    "logical offset " + logicalOffset + " is before first segment offset " + s.firstOffset);
        }
    }

    final String prefix;
    final List<String> indexPrefixes;

    FileChannel currentSegmentChannel;
    Map<String, AsyncIndexFile> currentIndexFiles;
    // Selected segment covers logical range [openedSegmentStartOffset, openedSegmentEndOffset).
    // Empty directory uses [0, Long.MAX_VALUE). Long.MAX_VALUE for end means the selected
    // segment is the tail — end is unbounded (writer may still append).
    long openedSegmentStartOffset;
    long openedSegmentEndOffset;
    // if true, means there are some segment files to be deleted caused by truncate or delete.
    // once delete io is completed, the flag will be cleared.
    volatile boolean mayHaveOrphanFiles;

    @Override
    FileChannel currentWriteChannel() {
        return currentSegmentChannel;
    }

    @Override
    long openCurrentChannel() throws IOException {
        final long startOffset = openedSegmentStartOffset;
        FileChannel oldChannel = null;
        FileChannel newChannel = null;
        try {
            long logicalOffset = -1;
            if (canWrite()) {
                newChannel = FileChannel.open(segmentPath(startOffset),
                        StandardOpenOption.WRITE, StandardOpenOption.CREATE);
                long physicalSize = newChannel.size();
                newChannel.position(physicalSize);
                logicalOffset = startOffset + physicalSize;
            } else {
                newChannel = FileChannel.open(segmentPath(startOffset),
                        StandardOpenOption.READ, StandardOpenOption.CREATE);
            }

            synchronized (this) {
                if (closed) {
                    // Closed while we were opening: the channel we just created must be released.
                    throw new IllegalStateException("file is closed: " + path);
                }
                oldChannel = currentSegmentChannel;
                currentSegmentChannel = newChannel;
                newChannel = null;
            }
            return logicalOffset;
        } finally {
            closeChannelQuietly(newChannel, "abandoned segment channel candidate");
            closeChannelQuietly(oldChannel, "replaced segment channel");
        }
    }

    private void closeChannelQuietly(FileChannel channel, String reason) {
        if (channel == null) {
            return;
        }
        try {
            channel.close();
        } catch (IOException e) {
            logger.error("failed to close {} for {} at offset {}", reason, path,
                    openedSegmentStartOffset, e);
        }
    }

    @Override
    SegmentFileCacheEntry getCacheEntry() {
        return (SegmentFileCacheEntry) cacheEntry;
    }

    void setCacheEntry(SegmentFileCacheEntry entry) {
        this.cacheEntry = entry;
        for (AsyncIndexFile af : currentIndexFiles.values()) {
            if (af.cacheEntry == null) {
                tryBindIndexFileCacheEntry(af);
            }
        }
        maybeBindWriterIndexLease();
    }

    private void tryBindIndexFileCacheEntry(AsyncIndexFile af) {
        try {
            bindIndexFileCacheEntry(af);
        } catch (RuntimeException e) {
            logger.error("failed to bind index file cache entry for {}, continue without cache", af.path, e);
        }
    }

    private void bindIndexFileCacheEntry(AsyncIndexFile af) {
        SegmentFileCacheEntry segmentEntry = getCacheEntry();
        if (segmentEntry == null) {
            return;
        }
        boolean write = af.canWrite();
        Pair<Boolean, FileCacheEntry> acquired =
                segmentEntry.acquireIndexFileCacheEntry(af.startOffset, af.indexPrefix, write);
        af.cacheEntry = acquired.getValue();
        af.firstOpener = acquired.getKey();
        af.onCacheClose = () -> segmentEntry.releaseIndexFileCacheEntry(
                af.startOffset, af.indexPrefix, write, af.getCacheEntry());
    }

    private void maybeBindWriterIndexLease() {
        SegmentFileCacheEntry segmentEntry = getCacheEntry();
        if (segmentEntry == null || !canWrite()) {
            return;
        }
        segmentEntry.bindWriterIndexLease(openedSegmentStartOffset);
    }

    // Single factory for index file metadata: creates the handle, binds its cache entry and
    // registers it in currentIndexFiles. No IO — channels are opened later by initIndexChannels.
    private AsyncIndexFile openIndexFile(String indexPrefix, long startOffset, boolean noFs) {
        String fileName = indexPrefix + startOffset;
        AsyncIndexFile af = new AsyncIndexFile(key, ioKey, absolutePathOf(fileName), indexPrefix, startOffset,
                canWrite() ? OpenMode.READ_WRITE : OpenMode.READ);
        af.needPrepare = needPrepare || noFs;
        tryBindIndexFileCacheEntry(af);
        currentIndexFiles.put(indexPrefix, af);
        return af;
    }

    AsyncSegmentFile(String dirPath, String prefix, List<String> indexPrefixes, String key, String ioKey,
            boolean writeMode) {
        super(writeMode ? OpenMode.WRITE : OpenMode.READ, false, key, ioKey,
                Paths.get(dirPath, prefix).toString(), dirPath);
        this.prefix = prefix;
        this.indexPrefixes = indexPrefixes;
        this.currentIndexFiles = new HashMap<>();
        markEmptyOpenedRange();
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
    // Sets up metadata (offsets, index file map, position) only.
    void openInitialResources(FileEntry entry) {
        SegmentDirState s = entry.state;
        if (s.isEmpty()) {
            if (canWrite()) {
                createNewSegmentMetadata(0, entry, needPrepare);
            }
            return;
        }

        if (canWrite()) {
            openedSegmentStartOffset = s.lastOffset;
            openedSegmentEndOffset = Long.MAX_VALUE;
            for (String indexPrefix : indexPrefixes) {
                openIndexFile(indexPrefix, openedSegmentStartOffset, needPrepare);
            }
        } else {
            position = s.firstOffset;
            // Reader: set offsets for the first segment without opening channel.
            Pair<Long, Long> range = s.floorKeyAndNext(position);
            openedSegmentStartOffset = range.getKey();
            openedSegmentEndOffset = range.getValue();
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

    // Detach all current channels into pending list, shall not close the channels in this method.
    @Override
    List<FileChannel> detachCurrentChannels() {
        List<FileChannel> pending = new ArrayList<>();
        if (currentSegmentChannel != null) {
            pending.add(currentSegmentChannel);
            currentSegmentChannel = null;
        }

        for (AsyncIndexFile af : currentIndexFiles.values()) {
            final boolean firstClose;
            List<FileChannel> afChannels = Collections.emptyList();
            synchronized (af) {
                firstClose = !af.closed;
                if (firstClose) {
                    af.closed = true;
                    afChannels = af.detachCurrentChannels();
                }
            }
            pending.addAll(afChannels);
            if (firstClose) {
                try {
                    af.onCacheClose.run();
                } catch (Throwable t) {
                    logger.error("onCacheClose failed for {}", af.path, t);
                }
            }
        }
        currentIndexFiles.clear();
        pendingFsyncBytes = 0;
        lastFsyncNanos = System.nanoTime();
        return pending;
    }

    private void markEmptyOpenedRange() {
        openedSegmentStartOffset = 0;
        openedSegmentEndOffset = Long.MAX_VALUE;
    }

    long exclusiveEndOffset(long lastOffset) throws IOException {
        return lastOffset + Files.size(segmentPath(lastOffset));
    }

    // Delete on-disk segment/index files whose offset is not in metadata. Metadata unchanged.
    void deleteOrphanFiles(FileEntry entry) throws IOException {
        List<String> names = StorageUtil.listNamesSync(Paths.get(dirPath));
        SegmentDirState s = entry.state;
        for (String name : names) {
            String matchedPrefix = null;
            if (name.startsWith(prefix)) {
                matchedPrefix = prefix;
            } else {
                for (String indexPrefix : indexPrefixes) {
                    if (name.startsWith(indexPrefix)) {
                        matchedPrefix = indexPrefix;
                        break;
                    }
                }
            }

            if (matchedPrefix == null) {
                continue;
            }

            try {
                long offset = Long.parseLong(name.substring(matchedPrefix.length()));
                if (!s.contains(offset)) {
                    logger.warn("Deleting orphan file in {}: {}", dirPath, name);
                    Files.deleteIfExists(Paths.get(dirPath, name));
                }
            } catch (NumberFormatException e) {
                logger.warn("Deleting unrecognized file in {}: {}", dirPath, name);
                Files.deleteIfExists(Paths.get(dirPath, name));
            }
        }
        mayHaveOrphanFiles = false;
    }

    // Roll metadata: detach old channels, update state/offsets/indexFiles for the new segment.
    List<FileChannel> rollMetadata(FileEntry entry, long currentSegmentSize, boolean noFs) {
        if (entry.state.isEmpty()) {
            throw new IllegalStateException("cannot roll segment file with empty state: " + path);
        }
        if (currentSegmentSize == 0) {
            return Collections.emptyList();
        }
        long newStartOffset = openedSegmentStartOffset + currentSegmentSize;
        List<FileChannel> oldChannels = detachCurrentChannels();
        List<FileChannel> created = createNewSegmentMetadata(newStartOffset, entry, noFs);
        oldChannels.addAll(created);
        return oldChannels;
    }

    void initCurrentChannels() throws IOException {
        // Readers open the segment channel lazily on read; nothing to do here.
        if (!canWrite()) {
            return;
        }
        if (currentSegmentChannel == null) {
            try {
                openCurrentChannel();
            } catch (IOException e) {
                // initCurrentChannels is called when open/truncate/roll only.
                // set channel to null to reopen it when initCurrentChannels is called again.
                currentSegmentChannel = null;
                throw e;
            }
        }
        initIndexChannels(currentIndexFiles.values());
    }

    void initIndexChannels(Collection<? extends AsyncFile> indexFiles) throws IOException {
        for (AsyncFile af : indexFiles) {
            if (af.needPrepare) {
                continue;
            }
            if (af.channel == null) {
                try {
                    af.openCurrentChannel();
                } catch (IOException e) {
                    // similar to initCurrentChannels
                    af.channel = null;
                    throw e;
                }
            }
        }
    }

    private List<FileChannel> createNewSegmentMetadata(long startOffset, FileEntry entry, boolean noFs) {
        openedSegmentStartOffset = startOffset;
        openedSegmentEndOffset = Long.MAX_VALUE;

        for (String indexPrefix : indexPrefixes) {
            openIndexFile(indexPrefix, startOffset, noFs);
        }
        entry.state = new SegmentDirState(entry.state.copyAppend(startOffset));
        maybeBindWriterIndexLease();
        return Collections.emptyList();
    }

    List<FileChannel> deleteMetadata(FileEntry entry) {
        List<FileChannel> oldChannels = detachCurrentChannels();
        markEmptyOpenedRange();
        if (!entry.state.isEmpty()) {
            // Set before publishing the new state (see mayHaveOrphanFiles).
            mayHaveOrphanFiles = true;
        }
        entry.state = SegmentDirState.EMPTY;
        return oldChannels;
    }

    long[] deleteSegmentsMetadata(long lastDeletedOffset, FileEntry entry) {
        SegmentDirState cur = entry.state;
        if (cur.isEmpty() || lastDeletedOffset < cur.firstOffset) {
            return SegmentDirState.EMPTY.offsets();
        }
        if (lastDeletedOffset >= cur.lastOffset) {
            throw new IllegalArgumentException("deleteSegments cannot delete the last segment at "
                    + cur.lastOffset + " with lastDeletedOffset " + lastDeletedOffset);
        }

        int lastDeletedIndex = cur.indexOf(lastDeletedOffset);
        if (lastDeletedIndex < 0) {
            throw new IllegalArgumentException("lastDeletedOffset is not a segment start offset: "
                    + lastDeletedOffset);
        }
        int drop = lastDeletedIndex + 1;
        long[] droppedOffsets = cur.copyShrink(drop);
        mayHaveOrphanFiles = true;
        entry.state = new SegmentDirState(cur.copyFrom(drop));
        return droppedOffsets;
    }

    void deleteSegmentAndIndexFiles(long[] offsets) throws IOException {
        for (long offset : offsets) {
            Files.deleteIfExists(segmentPath(offset));
            for (String indexPrefix : indexPrefixes) {
                Files.deleteIfExists(pathOf(indexPrefix + offset));
            }
        }
    }

    void truncateFileAndSync(long offset) throws IOException {
        long newSegmentSize = offset - openedSegmentStartOffset;
        currentSegmentChannel.truncate(newSegmentSize);
        currentSegmentChannel.position(newSegmentSize);
        currentSegmentChannel.force(true);
        pendingFsyncBytes = 0;
        lastFsyncNanos = System.nanoTime();
    }

    // ---- Reader metadata-only methods ----

    boolean openedSegmentMatchesState(SegmentDirState s, long logicalOffset) {
        if (s.isEmpty() || logicalOffset < s.firstOffset) {
            return false;
        }
        Pair<Long, Long> expected = s.floorKeyAndNext(logicalOffset);
        return expected.getKey() == openedSegmentStartOffset
                && expected.getValue() == openedSegmentEndOffset;
    }

    // Fast check: is the channel open and offset within the currently opened segment range
    // true does not mean logicalOffset is always within the opened segment range.
    boolean isSegmentReady(long logicalOffset) {
        return currentSegmentChannel != null
                && logicalOffset >= openedSegmentStartOffset
                && logicalOffset < openedSegmentEndOffset;
    }

    // always detach current channels before switch to a new segment.
    // caller shall check before calling this method.
    // Returns true when found a segment that actually covers logicalOffset
    // Returns false when it could not honour logicalOffset:
    //   - empty state: opened range is marked empty;
    //   - logicalOffset left of firstOffset: falls back to the first segment.
    // Detached channels are appended to pending for the caller to close.
    boolean switchToSegment(long logicalOffset, SegmentDirState s, List<FileChannel> pending) {
        pending.addAll(detachCurrentChannels());
        if (s.isEmpty()) {
            markEmptyOpenedRange();
            return false;
        }
        Pair<Long, Long> range = s.floorKeyAndNext(logicalOffset);
        boolean beforeFirst = range.getKey() < 0;
        if (beforeFirst) {
            // Left of the first segment: position on the first segment and report failure.
            range = s.floorKeyAndNext(s.firstOffset);
        }
        openedSegmentStartOffset = range.getKey();
        openedSegmentEndOffset = range.getValue();
        return !beforeFirst;
    }

    void openSegmentChannelForRead() throws IOException {
        if (currentSegmentChannel != null) {
            return;
        }
        openCurrentChannel();
    }

    Pair<Long, Map<String, AsyncIndexFile>> getCurrentIndexFiles(List<String> requestedPrefixes, boolean noFs) {
        Map<String, AsyncIndexFile> result = new HashMap<>();
        for (String indexPrefix : requestedPrefixes) {
            AsyncIndexFile af = currentIndexFiles.get(indexPrefix);
            if (af == null) {
                af = openIndexFile(indexPrefix, openedSegmentStartOffset, noFs);
            }
            result.put(indexPrefix, af);
        }
        return Pair.from(openedSegmentStartOffset, result);
    }

    long[] truncate(long offset, FileEntry entry, long endOffset, boolean noFs, List<FileChannel> pending) {
        SegmentDirState s = entry.state;
        if (!s.isEmpty() && offset >= s.firstOffset && offset <= endOffset) {
            return truncateInRange(offset, entry, noFs, pending);
        } else {
            return reset(offset, entry, noFs, pending);
        }
    }

    private long[] truncateInRange(long offset, FileEntry entry, boolean noFs, List<FileChannel> pending) {
        SegmentDirState cur = entry.state;
        long targetStart = cur.floorKey(offset);
        boolean reuseCurrent = openedSegmentStartOffset == targetStart;
        int cut = cur.indexOf(targetStart) + 1;
        long[] nextArr = cur.copyShrink(cut);

        if (!reuseCurrent) {
            pending.addAll(detachCurrentChannels());
            openedSegmentStartOffset = targetStart;
        }
        openedSegmentEndOffset = Long.MAX_VALUE;

        for (String indexPrefix : indexPrefixes) {
            if (currentIndexFiles.get(indexPrefix) == null) {
                openIndexFile(indexPrefix, targetStart, noFs);
            }
        }

        long[] dropped = cur.copyFrom(cut);
        if (dropped.length > 0) {
            mayHaveOrphanFiles = true;
        }
        entry.state = new SegmentDirState(nextArr);
        maybeBindWriterIndexLease();
        return dropped;
    }

    private long[] reset(long offset, FileEntry entry, boolean noFs, List<FileChannel> pending) {
        pending.addAll(detachCurrentChannels());
        SegmentDirState cur = entry.state;
        long[] dropped = cur.offsets();
        if (dropped.length > 0) {
            mayHaveOrphanFiles = true;
        }
        entry.state = SegmentDirState.EMPTY;
        createNewSegmentMetadata(offset, entry, noFs);
        return dropped;
    }

}

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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class AsyncSegmentFile {

    private static final Logger logger = LoggerFactory.getLogger(AsyncSegmentFile.class);

    final String dirPath;
    final String prefix;
    final List<IndexFileMapping> indexMappings;
    final boolean writeMode;

    FileChannel currentSegmentChannel;
    Map<String, FileChannel> currentIndexChannels;
    // Currently open segment covers logical range [openedSegmentStartOffset, openedSegmentEndOffset).
    // Long.MAX_VALUE for start means "no segment currently open"; Long.MAX_VALUE for end
    // means "open segment is the tail — end is unbounded (writer may still append)".
    long openedSegmentStartOffset = Long.MAX_VALUE;
    long openedSegmentEndOffset = Long.MAX_VALUE;
    long readPosition = 0;

    // start offsets of the maximal contiguous valid segment chain, ascending.
    // segment[i] covers [offsets[i], offsets[i+1] - 1]; last covers [offsets[last], max].
    final TreeSet<Long> segmentOffsets = new TreeSet<>();
    // offsetToFileName may use random numbers, so we must record the actual file names.
    final Map<Long, Map<String, String>> segmentIndexFiles = new HashMap<>();

    AsyncSegmentFile(String dirPath, String prefix, List<IndexFileMapping> indexMappings, boolean writeMode) {
        this.dirPath = dirPath;
        this.prefix = prefix;
        this.indexMappings = indexMappings;
        this.currentIndexChannels = new HashMap<>();
        this.writeMode = writeMode;
    }

    // Initialize from existing files, deleting any invalid files (segments outside the contiguous chain + orphan/duplicate index files).
    void initFromFiles(List<String> allFiles) throws IOException {
        List<long[]> segs = new ArrayList<>();
        // prefix -> offset -> list of matching filenames (may have duplicates mapping to same offset)
        Map<String, Map<Long, List<String>>> tempIndexFiles = new HashMap<>();
        List<String> invalidFiles = new ArrayList<>();

        for (String name : allFiles) {
            if (name.startsWith(prefix)) {
                try {
                    long offset = Long.parseLong(name.substring(prefix.length()));
                    long size = Files.size(pathOf(name));
                    segs.add(new long[]{offset, size});
                } catch (NumberFormatException e) {
                    invalidFiles.add(name);
                }
            } else {
                for (IndexFileMapping mapping : indexMappings) {
                    if (name.startsWith(mapping.prefix)) {
                        Long offset = mapping.fileNameToOffset.apply(name);
                        if (offset == null) {
                            invalidFiles.add(name);
                        } else {
                            tempIndexFiles
                                    .computeIfAbsent(mapping.prefix, k -> new HashMap<>())
                                    .computeIfAbsent(offset, k -> new ArrayList<>())
                                    .add(name);
                        }
                        break;
                    }
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
                } else if (segEnd > chainHead) {
                    logger.warn("Overlapping segment in {}: {} ends at {} but validOffsets is {}",
                            dirPath, prefix + seg[0], segEnd, validOffsets);
                    invalidFiles.add(prefix + seg[0]);
                } else {
                    invalidFiles.add(prefix + seg[0]);
                }
            }
        }

        segmentOffsets.addAll(validOffsets);

        for (long offset : validOffsets) {
            segmentIndexFiles.put(offset, new HashMap<>());
        }

        for (Map.Entry<String, Map<Long, List<String>>> e1 : tempIndexFiles.entrySet()) {
            String mappingPrefix = e1.getKey();
            for (Map.Entry<Long, List<String>> e2 : e1.getValue().entrySet()) {
                long offset = e2.getKey();
                List<String> files = e2.getValue();
                if (!validOffsets.contains(offset)) {
                    invalidFiles.addAll(files);
                } else {
                    String chosen;
                    if (files.size() == 1) {
                        chosen = files.get(0);
                    } else {
                        chosen = resolveLatest(files, invalidFiles);
                        logger.warn("Multiple index files for offset {} under prefix {} in {}: {}, keeping {}",
                                offset, mappingPrefix, dirPath, files, chosen);
                    }
                    segmentIndexFiles.get(offset).put(mappingPrefix, chosen);
                }
            }
        }

        if (!invalidFiles.isEmpty()) {
            logger.warn("Found {} invalid files in {}: {}", invalidFiles.size(), dirPath, invalidFiles);
            for (String name : invalidFiles) {
                Files.deleteIfExists(pathOf(name));
            }
        }

        if (!segmentOffsets.isEmpty()) {
            if (writeMode) {
                openedSegmentStartOffset = segmentOffsets.last();
                openedSegmentEndOffset = Long.MAX_VALUE;
                currentSegmentChannel = openForAppend(prefix + openedSegmentStartOffset);
                currentIndexChannels = new HashMap<>();
                Map<String, String> indexFiles = segmentIndexFiles.get(openedSegmentStartOffset);
                for (IndexFileMapping mapping : indexMappings) {
                    String fileName = indexFiles.get(mapping.prefix);
                    if (fileName == null) {
                        fileName = mapping.offsetToFileName.apply(openedSegmentStartOffset);
                        indexFiles.put(mapping.prefix, fileName);
                    }
                    currentIndexChannels.put(mapping.prefix, openForAppend(fileName));
                }
            } else {
                readPosition = segmentOffsets.first();
            }
        }
    }

    private String resolveLatest(List<String> files, List<String> invalidFiles) throws IOException {
        String winner = null;
        long winnerTime = -1;
        for (String f : files) {
            long modTime = Files.getLastModifiedTime(pathOf(f)).toMillis();
            if (modTime > winnerTime) {
                if (winner != null) invalidFiles.add(winner);
                winner = f;
                winnerTime = modTime;
            } else {
                invalidFiles.add(f);
            }
        }
        return winner;
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

    private FileChannel openForAppend(String fileName) throws IOException {
        FileChannel ch = FileChannel.open(pathOf(fileName),
                EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE));
        ch.position(ch.size());
        return ch;
    }

    void closeCurrent() throws IOException {
        if (currentSegmentChannel != null) currentSegmentChannel.close();
        currentSegmentChannel = null;
        for (FileChannel ch : currentIndexChannels.values()) ch.close();
        currentIndexChannels.clear();
        openedSegmentStartOffset = Long.MAX_VALUE;
        openedSegmentEndOffset = Long.MAX_VALUE;
    }

    long exclusiveEndOffset() throws IOException {
        long maxOffset = segmentOffsets.last();
        return maxOffset + Files.size(segmentPath(maxOffset));
    }

    private void deleteSegmentAndIndex(long offset) throws IOException {
        Files.deleteIfExists(segmentPath(offset));
        for (String indexFileName : segmentIndexFiles.get(offset).values()) {
            Files.deleteIfExists(pathOf(indexFileName));
        }
    }

    private Map<String, AsyncFile> createNewSegmentWithIndexes(long startOffset) throws IOException {
        openedSegmentStartOffset = startOffset;
        openedSegmentEndOffset = Long.MAX_VALUE;
        segmentOffsets.add(startOffset);
        Map<String, String> indexFileNames = new HashMap<>();
        segmentIndexFiles.put(startOffset, indexFileNames);
        currentSegmentChannel = FileChannel.open(segmentPath(startOffset),
                StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        Map<String, AsyncFile> result = new HashMap<>();
        for (IndexFileMapping mapping : indexMappings) {
            String fileName = mapping.offsetToFileName.apply(startOffset);
            indexFileNames.put(mapping.prefix, fileName);
            FileChannel ch = FileChannel.open(pathOf(fileName),
                    EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW));
            currentIndexChannels.put(mapping.prefix, ch);
            result.put(mapping.prefix, new AsyncFile(absolutePathOf(fileName), ch, false, true));
        }
        return result;
    }

    boolean switchToSegment(long logicalOffset) throws IOException {
        if (logicalOffset >= openedSegmentStartOffset && logicalOffset < openedSegmentEndOffset
                && currentSegmentChannel != null) {
            return true;
        }
        if (segmentOffsets.isEmpty()) {
            closeCurrent();
            return false;
        }
        if (logicalOffset < segmentOffsets.first()) {
            closeCurrent();
            return false;
        }
        long exclusiveEnd = exclusiveEndOffset();
        if (logicalOffset >= exclusiveEnd) {
            closeCurrent();
            return false;
        }
        long segStart = segmentOffsets.floor(logicalOffset);
        closeCurrent();
        currentSegmentChannel = FileChannel.open(
                segmentPath(segStart), StandardOpenOption.READ);
        openedSegmentStartOffset = segStart;
        if (segStart == segmentOffsets.last()) {
            openedSegmentEndOffset = Long.MAX_VALUE;
        } else {
            openedSegmentEndOffset = segStart + Files.size(segmentPath(segStart));
        }
        return true;
    }

    // lazy-create the first segment (starting at offset 0) and its index files for write mode.
    void openFirstSegmentChannelForWrite() throws IOException {
        createNewSegmentWithIndexes(0);
    }

    Map<String, AsyncFile> getCurrentIndexFiles(List<String> indexPrefixes) throws IOException {
        Map<String, String> indexFileNames = segmentIndexFiles.get(openedSegmentStartOffset);
        Map<String, AsyncFile> result = new HashMap<>();

        for (String prefix : indexPrefixes) {
            String fileName = indexFileNames.get(prefix);
            if (fileName == null) {
                continue;
            }

            FileChannel ch = currentIndexChannels.get(prefix);
            if (ch != null) {
                result.put(prefix, new AsyncFile(absolutePathOf(fileName), ch, false, writeMode));
            } else if (writeMode) {
                logger.error("Index channel for prefix {} is null in write mode, segment offset: {}", prefix, openedSegmentStartOffset);
                throw new IllegalStateException("Index channel for prefix " + prefix + " is null in write mode");
            } else {
                FileChannel readCh = FileChannel.open(pathOf(fileName), StandardOpenOption.READ);
                currentIndexChannels.put(prefix, readCh);
                result.put(prefix, new AsyncFile(absolutePathOf(fileName), readCh, false, false));
            }
        }

        return result;
    }

    void deleteSegments(List<Long> startOffsets) throws IOException {
        for (long offset : startOffsets) {
            long first = segmentOffsets.first();
            if (offset != first) {
                throw new IllegalArgumentException("deleteSegments requires deleting segments in order from the first: expected " + first + ", got " + offset);
            }
            if (segmentOffsets.size() <= 1) {
                throw new IllegalArgumentException("deleteSegments cannot delete the last segment");
            }

            deleteSegmentAndIndex(offset);
            segmentOffsets.remove(offset);
            segmentIndexFiles.remove(offset);
        }
    }

    void delete() throws IOException {
        for (long offset : new ArrayList<>(segmentOffsets)) {
            deleteSegmentAndIndex(offset);
        }
        segmentOffsets.clear();
        segmentIndexFiles.clear();
    }

    Map<String, AsyncFile> truncate(long offset) throws IOException {
        if (!segmentOffsets.isEmpty()
                && offset >= segmentOffsets.first() && offset <= exclusiveEndOffset()) {
            return truncateInRange(offset);
        }
        return reset(offset);
    }

    private Map<String, AsyncFile> truncateInRange(long offset) throws IOException {
        long targetStart = segmentOffsets.floor(offset);
        boolean reuseCurrent = openedSegmentStartOffset == targetStart && currentSegmentChannel != null;

        if (!reuseCurrent) {
            closeCurrent();
            openedSegmentStartOffset = targetStart;
            currentSegmentChannel = FileChannel.open(
                    segmentPath(targetStart), StandardOpenOption.WRITE);
        }

        for (long o : new ArrayList<>(segmentOffsets.tailSet(targetStart, false))) {
            deleteSegmentAndIndex(o);
            segmentOffsets.remove(o);
            segmentIndexFiles.remove(o);
        }

        currentSegmentChannel.truncate(offset - targetStart);
        currentSegmentChannel.position(offset - targetStart);

        Map<String, String> indexFileNames = segmentIndexFiles.get(targetStart);
        Map<String, AsyncFile> result = new HashMap<>();
        for (IndexFileMapping mapping : indexMappings) {
            String fileName = indexFileNames.get(mapping.prefix);
            if (fileName == null) {
                fileName = mapping.offsetToFileName.apply(targetStart);
                indexFileNames.put(mapping.prefix, fileName);
            }
            FileChannel ch = currentIndexChannels.get(mapping.prefix);
            if (ch == null) {
                ch = openForAppend(fileName);
                currentIndexChannels.put(mapping.prefix, ch);
            }
            result.put(mapping.prefix, new AsyncFile(absolutePathOf(fileName), ch, false, true));
        }
        return result;
    }

    private Map<String, AsyncFile> reset(long offset) throws IOException {
        closeCurrent();

        for (long o : new ArrayList<>(segmentOffsets)) {
            deleteSegmentAndIndex(o);
        }
        segmentOffsets.clear();
        segmentIndexFiles.clear();

        return createNewSegmentWithIndexes(offset);
    }

    Map<String, AsyncFile> roll() throws IOException {
        long newStartOffset = openedSegmentStartOffset + currentSegmentChannel.size();
        closeCurrent();
        return createNewSegmentWithIndexes(newStartOffset);
    }
}

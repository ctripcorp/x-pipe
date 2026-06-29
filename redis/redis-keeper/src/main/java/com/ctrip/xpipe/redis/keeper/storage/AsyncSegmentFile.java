package com.ctrip.xpipe.redis.keeper.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
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
    long currentSegmentStartOffset;

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
        this.currentSegmentStartOffset = 0;
    }

    // Returns the list of invalid files (segments outside the contiguous chain + orphan/duplicate index files).
    List<String> initFromFiles(List<String> allFiles) {
        List<long[]> segs = new ArrayList<>();
        // prefix -> offset -> list of matching filenames (may have duplicates mapping to same offset)
        Map<String, Map<Long, List<String>>> tempIndexFiles = new HashMap<>();
        List<String> invalidFiles = new ArrayList<>();

        for (String name : allFiles) {
            if (name.startsWith(prefix)) {
                try {
                    long offset = Long.parseLong(name.substring(prefix.length()));
                    long size = Files.size(Paths.get(dirPath, name));
                    segs.add(new long[]{offset, size});
                } catch (NumberFormatException e) {
                    invalidFiles.add(name);
                } catch (IOException e) {
                    throw new StorageIOException(e);
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
        }

        if (!segmentOffsets.isEmpty()) {
            if (writeMode) {
                currentSegmentStartOffset = segmentOffsets.last();
                try {
                    currentSegmentChannel = openWrite(prefix + currentSegmentStartOffset);
                    currentIndexChannels = new HashMap<>();
                    Map<String, String> indexFiles = segmentIndexFiles.get(currentSegmentStartOffset);
                    for (IndexFileMapping mapping : indexMappings) {
                        String fileName = indexFiles.get(mapping.prefix);
                        if (fileName == null) {
                            fileName = mapping.offsetToFileName.apply(currentSegmentStartOffset);
                            indexFiles.put(mapping.prefix, fileName);
                        }
                        currentIndexChannels.put(mapping.prefix, openWrite(fileName));
                    }
                } catch (IOException e) {
                    throw new StorageIOException(e);
                }
            } else {
                currentSegmentStartOffset = segmentOffsets.first();
            }
        }

        return invalidFiles;
    }

    private String resolveLatest(List<String> files, List<String> invalidFiles) {
        String winner = null;
        long winnerTime = -1;
        for (String f : files) {
            long modTime;
            try {
                modTime = Files.getLastModifiedTime(Paths.get(dirPath, f)).toMillis();
            } catch (IOException e) {
                throw new StorageIOException(e);
            }
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

    private FileChannel openWrite(String fileName) throws IOException {
        FileChannel ch = FileChannel.open(Paths.get(dirPath, fileName),
                EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE));
        ch.position(ch.size());
        return ch;
    }

    void switchToSegment(long logicalOffset) throws IOException {
        if (segmentOffsets.isEmpty()) {
            throw new IOException("No segments available");
        }

        long minOffset = segmentOffsets.first();
        long maxOffset = segmentOffsets.last();

        if (logicalOffset < minOffset) {
            throw new IOException("logicalOffset " + logicalOffset + " is less than minimum segment offset " + minOffset);
        }

        long lastSegmentSize = Files.size(Paths.get(dirPath, prefix + maxOffset));
        long maxValidOffset = maxOffset + lastSegmentSize;

        if (logicalOffset >= maxValidOffset) {
            throw new IOException("logicalOffset " + logicalOffset + " is >= max valid offset " + maxValidOffset);
        }

        Long segStart = segmentOffsets.floor(logicalOffset);

        if (segStart != currentSegmentStartOffset) {
            if (currentSegmentChannel != null) currentSegmentChannel.close();
            for (FileChannel ch : currentIndexChannels.values()) {
                ch.close();
            }
            currentIndexChannels.clear();
            currentSegmentStartOffset = segStart;
            currentSegmentChannel = FileChannel.open(
                    Paths.get(dirPath, prefix + segStart), StandardOpenOption.READ);
        }
    }

    Map<String, AsyncFile> getCurrentIndexFiles(List<String> indexPrefixes) throws IOException {
        Map<String, String> indexFileNames = segmentIndexFiles.get(currentSegmentStartOffset);
        Map<String, AsyncFile> result = new HashMap<>();

        for (String prefix : indexPrefixes) {
            String fileName = indexFileNames.get(prefix);
            if (fileName == null) {
                continue;
            }

            FileChannel ch = currentIndexChannels.get(prefix);
            if (ch != null) {
                result.put(prefix, new AsyncFile(dirPath + File.separator + fileName, ch));
            } else if (writeMode) {
                logger.error("Index channel for prefix {} is null in write mode, segment offset: {}", prefix, currentSegmentStartOffset);
                throw new IOException("Index channel for prefix " + prefix + " is null in write mode");
            } else {
                FileChannel readCh = FileChannel.open(Paths.get(dirPath, fileName), StandardOpenOption.READ);
                currentIndexChannels.put(prefix, readCh);
                result.put(prefix, new AsyncFile(dirPath + File.separator + fileName, readCh));
            }
        }

        return result;
    }

    Map<String, AsyncFile> getCurrentIndexFiles() throws IOException {
        List<String> prefixes = new ArrayList<>();
        for (IndexFileMapping mapping : indexMappings) {
            prefixes.add(mapping.prefix);
        }
        return getCurrentIndexFiles(prefixes);
    }

    long size() throws IOException {
        if (segmentOffsets.isEmpty()) {
            return 0;
        }
        long minOffset = segmentOffsets.first();
        long maxOffset = segmentOffsets.last();
        long lastSegmentSize = Files.size(Paths.get(dirPath, prefix + maxOffset));
        return maxOffset + lastSegmentSize - minOffset;
    }

    void deleteSegments(List<Long> startOffsets) throws IOException {
        for (long offset : startOffsets) {
            long first = segmentOffsets.first();
            if (offset != first) {
                throw new IOException("deleteSegments requires deleting segments in order from the first: expected " + first + ", got " + offset);
            }
            if (segmentOffsets.size() <= 1) {
                throw new IOException("deleteSegments cannot delete the last segment");
            }

            Files.deleteIfExists(Paths.get(dirPath, prefix + offset));
            Map<String, String> indexFiles = segmentIndexFiles.get(offset);
            if (indexFiles != null) {
                for (String indexFileName : indexFiles.values()) {
                    Files.deleteIfExists(Paths.get(dirPath, indexFileName));
                }
            }
            segmentOffsets.remove(offset);
            segmentIndexFiles.remove(offset);
        }
    }

    void delete() throws IOException {
        for (long offset : new ArrayList<>(segmentOffsets)) {
            Files.deleteIfExists(Paths.get(dirPath, prefix + offset));
            Map<String, String> indexFiles = segmentIndexFiles.get(offset);
            if (indexFiles != null) {
                for (String indexFileName : indexFiles.values()) {
                    Files.deleteIfExists(Paths.get(dirPath, indexFileName));
                }
            }
        }
        segmentOffsets.clear();
        segmentIndexFiles.clear();
    }

    Map<String, AsyncFile> roll() throws IOException {
        currentSegmentStartOffset = currentSegmentStartOffset + currentSegmentChannel.size();
        currentSegmentChannel.close();
        for (FileChannel ch : currentIndexChannels.values()) {
            ch.close();
        }
        currentIndexChannels.clear();
        currentSegmentChannel = FileChannel.open(
                Paths.get(dirPath, prefix + currentSegmentStartOffset),
                StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);

        Map<String, String> indexFileNames = new HashMap<>();
        segmentIndexFiles.put(currentSegmentStartOffset, indexFileNames);
        Map<String, AsyncFile> result = new HashMap<>();
        for (IndexFileMapping mapping : indexMappings) {
            String fileName = mapping.offsetToFileName.apply(currentSegmentStartOffset);
            indexFileNames.put(mapping.prefix, fileName);
            FileChannel ch = FileChannel.open(
                    Paths.get(dirPath, fileName),
                    EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW));
            currentIndexChannels.put(mapping.prefix, ch);
            result.put(mapping.prefix, new AsyncFile(dirPath + File.separator + fileName, ch));
        }
        return result;
    }
}

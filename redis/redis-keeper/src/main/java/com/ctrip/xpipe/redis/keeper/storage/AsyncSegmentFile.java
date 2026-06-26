package com.ctrip.xpipe.redis.keeper.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.ConcurrentSkipListSet;

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
    final ConcurrentSkipListSet<Long> segmentOffsets = new ConcurrentSkipListSet<>();

    AsyncSegmentFile(String dirPath, String prefix, List<IndexFileMapping> indexMappings, boolean writeMode) {
        this.dirPath = dirPath;
        this.prefix = prefix;
        this.indexMappings = indexMappings;
        this.writeMode = writeMode;
        this.currentSegmentStartOffset = 0;
    }

    // Returns the list of invalid files (segments outside the contiguous chain + orphan/duplicate index files).
    List<String> initFromFiles(List<String> allFiles) {
        List<long[]> segs = new ArrayList<>();
        // mapping -> offset -> list of matching filenames (may have duplicates mapping to same offset)
        Map<IndexFileMapping, Map<Long, List<String>>> tempIndexFiles = new HashMap<>();
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
                                    .computeIfAbsent(mapping, k -> new HashMap<>())
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
                    logger.warn("Overlapping segment in {}: {} ends at {} but chainHead is {}",
                            dirPath, prefix + seg[0], segEnd, chainHead);
                    invalidFiles.add(prefix + seg[0]);
                } else {
                    invalidFiles.add(prefix + seg[0]);
                }
            }
        }

        segmentOffsets.addAll(validOffsets);

        for (Map.Entry<IndexFileMapping, Map<Long, List<String>>> e1 : tempIndexFiles.entrySet()) {
            IndexFileMapping mapping = e1.getKey();
            for (Map.Entry<Long, List<String>> e2 : e1.getValue().entrySet()) {
                long offset = e2.getKey();
                List<String> files = e2.getValue();
                String chosen;
                if (files.size() == 1) {
                    chosen = files.get(0);
                } else {
                    chosen = resolveLatest(files, invalidFiles);
                    logger.warn("Multiple index files for offset {} under prefix {} in {}: {}, keeping {}",
                            offset, mapping.prefix, dirPath, files, chosen);
                }
                if (!validOffsets.contains(offset)) {
                    invalidFiles.add(chosen);
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
                    for (IndexFileMapping mapping : indexMappings) {
                        currentIndexChannels.put(mapping.prefix,
                                openWrite(mapping.offsetToFileName.apply(currentSegmentStartOffset)));
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
                winnerTime = modTime;
                winner = f;
            }
        }
        for (String f : files) {
            if (!f.equals(winner)) {
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
}

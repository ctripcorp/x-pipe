package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;
import com.ctrip.xpipe.redis.keeper.store.AsyncCommandStore;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.ctrip.xpipe.redis.keeper.store.gtid.index.AbstractIndex.BLOCK;
import static com.ctrip.xpipe.redis.keeper.store.gtid.index.AbstractIndex.BLOCK_V2;
import static com.ctrip.xpipe.redis.keeper.store.gtid.index.AbstractIndex.INDEX;
import static com.ctrip.xpipe.redis.keeper.store.gtid.index.AbstractIndex.INDEX_V2;

/**
 * Minimal {@link AsyncCommandStore} for index store unit tests.
 */
public class TestAsyncCommandStore implements AsyncCommandStore {

    private final AsyncFileSystem asyncFileSystem;
    private final AsyncSegmentFile asyncSegmentFile;
    private final File baseDir;
    private final String commandFileNamePrefix;
    private final List<String> commandIndexPrefixes;
    private final ReplId replId;
    private final int asyncWriteMaxBytes;

    public TestAsyncCommandStore(AsyncFileSystem asyncFileSystem, AsyncSegmentFile asyncSegmentFile, File baseDir,
                                 String commandFileNamePrefix) {
        this(asyncFileSystem, asyncSegmentFile, baseDir, commandFileNamePrefix, 65536);
    }

    public TestAsyncCommandStore(AsyncFileSystem asyncFileSystem, AsyncSegmentFile asyncSegmentFile, File baseDir,
                                 String commandFileNamePrefix, int asyncWriteMaxBytes) {
        this.asyncFileSystem = asyncFileSystem;
        this.asyncSegmentFile = asyncSegmentFile;
        this.baseDir = baseDir;
        this.commandFileNamePrefix = commandFileNamePrefix;
        this.asyncWriteMaxBytes = asyncWriteMaxBytes;
        this.replId = new ReplId("test-repl", 0L);
        this.commandIndexPrefixes = Arrays.asList(
                INDEX + commandFileNamePrefix,
                BLOCK + commandFileNamePrefix,
                INDEX_V2 + commandFileNamePrefix,
                BLOCK_V2 + commandFileNamePrefix);
    }

    @Override
    public AsyncFileSystem getAsyncFileSystem() {
        return asyncFileSystem;
    }

    @Override
    public AsyncSegmentFile getAsyncSegmentFile() {
        return asyncSegmentFile;
    }

    @Override
    public Map<String, AsyncFile> truncateCmdSegment(long cmdSegmentOffset) throws IOException {
        long globalOffset = getCurrentSegmentStartOffset() + cmdSegmentOffset;
        return AsyncFileSystemHelper.await(
                asyncFileSystem.truncate(asyncSegmentFile, globalOffset),
                "truncate test command segment");
    }

    @Override
    public long getCurrentSegmentStartOffset() throws IOException {
        long start = asyncFileSystem.getCurrentSegmentStartOffset(asyncSegmentFile);
        if (start < 0) {
            List<Long> offsets = asyncFileSystem.list(asyncSegmentFile);
            start = offsets.isEmpty() ? 0 : offsets.get(offsets.size() - 1);
        }
        return start;
    }

    @Override
    public AsyncSegmentFile getWriteSegmentFile() {
        return asyncSegmentFile;
    }

    @Override
    public Map<String, AsyncFile> truncateIndex(String indexPrefix, String blockPrefix,
                                                long indexSize, long blockSize) throws IOException {
        List<String> prefixes = Arrays.asList(indexPrefix, blockPrefix);
        Map<String, AsyncFile> indexMap = AsyncFileSystemHelper.await(
                asyncFileSystem.getCurrentIndexFiles(asyncSegmentFile, prefixes),
                "get test index files for truncate");
        truncateIndexFileIfPresent(indexMap, indexPrefix, indexSize);
        truncateIndexFileIfPresent(indexMap, blockPrefix, blockSize);
        return indexMap;
    }

    private void truncateIndexFileIfPresent(Map<String, AsyncFile> indexMap, String prefix, long size)
            throws IOException {
        AsyncFile indexFile = indexMap.get(prefix);
        if (indexFile != null && size >= 0) {
            AsyncFileSystemHelper.await(asyncFileSystem.truncate(indexFile, size),
                    "truncate test index file " + prefix);
        }
    }

    @Override
    public long currentSegmentSize() throws IOException {
        long start = getCurrentSegmentStartOffset();
        File cmdFile = new File(baseDir, commandFileNamePrefix + start);
        return cmdFile.exists() ? cmdFile.length() : 0;
    }

    @Override
    public File getCommandBaseDir() {
        return baseDir;
    }

    @Override
    public String getCommandFileNamePrefix() {
        return commandFileNamePrefix;
    }

    @Override
    public List<String> getCommandIndexPrefixes() {
        return commandIndexPrefixes;
    }

    @Override
    public ReplId getFileSystemReplId() {
        return replId;
    }

    @Override
    public int getAsyncWriteMaxBytes() {
        return asyncWriteMaxBytes;
    }
}

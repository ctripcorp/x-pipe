package com.ctrip.xpipe.redis.keeper.storage;

public class AsyncIndexFile extends AsyncFile {

    final String indexPrefix;
    final long startOffset;
    boolean firstOpener;

    AsyncIndexFile(String segmentKey, String segmentIoKey, String absolutePath, String indexPrefix,
            long startOffset, OpenMode openMode) {
        super(absolutePath, false, openMode, false, segmentKey, segmentIoKey, false);
        this.indexPrefix = indexPrefix;
        this.startOffset = startOffset;
        this.cacheMode = CacheMode.FULL_CACHE;
    }
}

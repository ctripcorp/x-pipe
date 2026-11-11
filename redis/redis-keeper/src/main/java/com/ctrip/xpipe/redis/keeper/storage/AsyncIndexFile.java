package com.ctrip.xpipe.redis.keeper.storage;

public class AsyncIndexFile extends AsyncFile {

    final String indexPrefix;
    final long startOffset;
    boolean firstOpener;

    AsyncIndexFile(String segmentKey, String segmentIoKey, String absolutePath, String indexPrefix,
            long startOffset, OpenMode openMode, String tenant) {
        super(absolutePath, ReplaceMode.NORMAL, openMode, false, segmentKey, segmentIoKey, false, tenant);
        this.indexPrefix = indexPrefix;
        this.startOffset = startOffset;
        this.cacheMode = CacheMode.FULL_CACHE;
    }
}

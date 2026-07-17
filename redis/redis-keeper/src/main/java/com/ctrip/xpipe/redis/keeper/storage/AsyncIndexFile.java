package com.ctrip.xpipe.redis.keeper.storage;

public class AsyncIndexFile extends AsyncFile {

    final String key;
    final String indexPrefix;
    final long startOffset;

    AsyncIndexFile(String segmentKey, String absolutePath, String indexPrefix, long startOffset, OpenMode openMode) {
        super(absolutePath, false, openMode, false);
        this.key = segmentKey + "\0" + indexPrefix + "\0" + startOffset;
        this.indexPrefix = indexPrefix;
        this.startOffset = startOffset;
        this.cacheMode = CacheMode.FULL_CACHE;
    }

    @Override
    String getKey() {
        return key;
    }
}

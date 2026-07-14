package com.ctrip.xpipe.redis.keeper.storage;

public class AsyncIndexFile extends AsyncFile {

    final String key;
    final String indexPrefix;
    final long startOffset;

    AsyncIndexFile(String key, String absolutePath, String indexPrefix, long startOffset, OpenMode openMode) {
        super(absolutePath, false, openMode);
        this.key = key;
        this.indexPrefix = indexPrefix;
        this.startOffset = startOffset;
        this.cacheMode = CacheMode.FULL_CACHE;
    }

    @Override
    String getKey() {
        return key;
    }

    @Override
    String identifier() {
        return key + "\0" + indexPrefix + "\0" + startOffset;
    }
}

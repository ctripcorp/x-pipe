package com.ctrip.xpipe.redis.keeper.storage;

import java.nio.channels.FileChannel;

public class AsyncIndexFile extends AsyncFile {

    final String key;
    final String indexPrefix;
    final long startOffset;

    AsyncIndexFile(String key, String absolutePath, String indexPrefix, long startOffset, FileChannel channel, boolean writeMode) {
        super(absolutePath, channel, false, writeMode);
        this.key = key;
        this.indexPrefix = indexPrefix;
        this.startOffset = startOffset;
        this.fullCacheOnly = true;
    }

    @Override
    String identifier() {
        return key;
    }
}

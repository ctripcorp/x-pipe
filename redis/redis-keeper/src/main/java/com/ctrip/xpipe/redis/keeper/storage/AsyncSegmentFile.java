package com.ctrip.xpipe.redis.keeper.storage;

import java.util.List;

public class AsyncSegmentFile extends AsyncFile {

    final String dirPath;
    final String prefix;
    final List<String> indexPrefixes;
    final boolean writeMode;

    long logicalPosition;
    long currentSegmentStartOffset;

    AsyncSegmentFile(String dirPath, String prefix, List<String> indexPrefixes, boolean writeMode) {
        super(null, null);
        this.dirPath = dirPath;
        this.prefix = prefix;
        this.indexPrefixes = indexPrefixes;
        this.writeMode = writeMode;
        this.logicalPosition = 0;
        this.currentSegmentStartOffset = -1;
    }
}

package com.ctrip.xpipe.redis.keeper.storage;

import java.util.function.Function;

public class IndexFileMapping {

    public final String prefix;
    public final Function<String, Long> fileNameToOffset;
    public final Function<Long, String> offsetToFileName;

    public IndexFileMapping(String prefix,
            Function<String, Long> fileNameToOffset,
            Function<Long, String> offsetToFileName) {
        this.prefix = prefix;
        this.fileNameToOffset = fileNameToOffset;
        this.offsetToFileName = offsetToFileName;
    }
}

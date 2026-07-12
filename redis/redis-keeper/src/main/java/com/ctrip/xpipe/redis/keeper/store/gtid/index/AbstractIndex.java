package com.ctrip.xpipe.redis.keeper.store.gtid.index;

public final class AbstractIndex {

    public static final String BLOCK = "block_";
    public static final String INDEX = "index_";

    public static final String INDEX_V2 = "indexv2_";
    public static final String BLOCK_V2 = "blockv2_";

    private AbstractIndex() {
    }

    public static long extractOffset(String fileName) {
        if (fileName.contains("_")) {
            return Long.parseLong(fileName.substring(fileName.lastIndexOf("_") + 1));
        }
        return Long.parseLong(fileName);
    }
}

package com.ctrip.xpipe.redis.keeper.store.gtid.index;

public class ContinuePoint {

    private String fileName;
    private long offset;

    public ContinuePoint(String fileName, long offset) {
        this.fileName = fileName;
        this.offset = offset;
    }

    public String getFileName() {
        return fileName;
    }

    public long getOffset() {
        return offset;
    }
}

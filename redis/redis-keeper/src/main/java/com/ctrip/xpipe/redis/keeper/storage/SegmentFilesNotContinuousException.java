package com.ctrip.xpipe.redis.keeper.storage;

public class SegmentFilesNotContinuousException extends RuntimeException {

    public SegmentFilesNotContinuousException(String message) {
        super(message);
    }
}

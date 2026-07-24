package com.ctrip.xpipe.redis.keeper.storage;

/**
 * Thrown when a logical offset is strictly less than the first segment start offset.
 * The previous opened range is retained, but segment resources are closed.
 */
public class SegmentOffsetBeforeFirstException extends RuntimeException {

    public SegmentOffsetBeforeFirstException(String message) {
        super(message);
    }
}

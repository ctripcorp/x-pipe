package com.ctrip.xpipe.redis.proxy.compress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Result {

    private static final Logger logger = LoggerFactory.getLogger(Result.class);

    private TimeInterval compressInterval, decompressInterval;

    private long compressedSize = 0L;

    public TimeInterval getCompressInterval() {
        return compressInterval;
    }

    public Result setCompressInterval(TimeInterval compressInterval) {
        this.compressInterval = compressInterval;
        return this;
    }

    public TimeInterval getDecompressInterval() {
        return decompressInterval;
    }

    public Result setDecompressInterval(TimeInterval decompressInterval) {
        this.decompressInterval = decompressInterval;
        return this;
    }

    public long getCompressedSize() {
        return compressedSize;
    }

    public Result setCompressedSize(long compressedSize) {
        this.compressedSize = compressedSize;
        return this;
    }

    public void printResult() {
        logger.info("compress:");
        logger.info("duration: {} ns", compressInterval.duration());
        logger.info("ratio: {} mb", (getCompressedSize()/1024/1024));
        logger.info("decompress:");
        logger.info("duration: {} ns", decompressInterval.duration());
    }

    public static class TimeInterval {

        private long start, end;

        public TimeInterval(long start, long end) {
            this.start = start;
            this.end = end;
        }

        public long duration() {
            return end - start;
        }
    }

}

package com.ctrip.xpipe.redis.comparator.config;

/**
 * redis-comparator 代码常量（spec §5）。禁止魔法值散落。
 */
public final class ComparatorConstants {

    public static final int STREAM_RECONNECT_MIN_MILLI = 1000;

    public static final int STREAM_RECONNECT_MAX_MILLI = 30000;

    public static final int REPLCONF_ACK_INTERVAL_MILLI = 1000;

    public static final int COMPARE_WAIT_MILLI = 100;

    public static final int COMPARE_STOP_JOIN_MILLI = 1000;

    public static final int COMPARE_THREAD_WARN_THRESHOLD = 500;

    public static final int MISMATCH_LOG_MIN_INTERVAL_MILLI = 60000;

    private ComparatorConstants() {
    }
}

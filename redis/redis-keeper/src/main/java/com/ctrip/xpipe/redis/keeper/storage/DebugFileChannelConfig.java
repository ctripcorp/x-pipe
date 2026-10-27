package com.ctrip.xpipe.redis.keeper.storage;

/**
 * Fault settings for the debug file channel (keeper-tfs-m6 §17.3). Reloaded by
 * {@link FileFaultInjector} once per refresh interval.
 */
public interface DebugFileChannelConfig {

    /** Whether the operation fails with an EIO after the delay. */
    boolean isEio();

    /** Delay added before each operation; ≤ 0 disables it. */
    long getDelayMilli();

    /**
     * Comma separated {@link FileFaultInjector.Category} names, case insensitive, unknown names
     * ignored; "*" means every category. Empty or null means none.
     */
    String getOps();

    /** When non empty, only paths containing it are injected. */
    String getPathContains();
}

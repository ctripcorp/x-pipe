package com.ctrip.xpipe.redis.meta.server.tfs;

/**
 * Shared TFS state-change step settings (§4.5: 1000ms timeout, 0 retry).
 */
public final class TfsCommandConstants {

    public static final int TFS_STEP_TIMEOUT_MILLI = 1000;

    public static final int TFS_STEP_RETRY_TIMES = 0;

    private TfsCommandConstants() {
    }
}

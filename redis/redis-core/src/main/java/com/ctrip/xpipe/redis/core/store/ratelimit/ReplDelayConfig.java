package com.ctrip.xpipe.redis.core.store.ratelimit;

public interface ReplDelayConfig {

    default long getDelayMilli() {
        return 0;
    }

    default int getFsyncLimitPerSecond() {
        return -1;
    }

    default int getPsyncLimitPerSecond() {
        return -1;
    }

}

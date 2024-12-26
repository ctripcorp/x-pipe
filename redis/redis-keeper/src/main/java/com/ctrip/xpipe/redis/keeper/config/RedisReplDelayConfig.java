package com.ctrip.xpipe.redis.keeper.config;

public class RedisReplDelayConfig {

    private int bytesLimitPerSecond = -1;

    public int getBytesLimitPerSecond() {
        return bytesLimitPerSecond;
    }

    public void setBytesLimitPerSecond(int bytesLimitPerSecond) {
        this.bytesLimitPerSecond = bytesLimitPerSecond;
    }
}

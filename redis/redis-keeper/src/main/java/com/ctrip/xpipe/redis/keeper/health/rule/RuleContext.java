package com.ctrip.xpipe.redis.keeper.health.rule;

import com.ctrip.xpipe.redis.keeper.storage.health.IoHealthSnapshot;

public final class RuleContext {

    private final IoHealthSnapshot snapshot;
    private final RuleConfig config;
    private final long nowNanos;

    public RuleContext(IoHealthSnapshot snapshot, RuleConfig config, long nowNanos) {
        this.snapshot = snapshot;
        this.config = config;
        this.nowNanos = nowNanos;
    }

    public IoHealthSnapshot snapshot() {
        return snapshot;
    }

    public RuleConfig config() {
        return config;
    }

    public long nowNanos() {
        return nowNanos;
    }
}

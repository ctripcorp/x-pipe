package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisHealthCheckInstance;

/**
 * @author lishanglin
 * date 2024/2/5
 */
public class NoRedisToSubContext extends SentinelActionContext {

    private static final Throwable th = new XpipeRuntimeException("No Redis to sub.");

    private String cluster;

    private String shard;

    public NoRedisToSubContext(String cluster, String shard) {
        super(new DefaultRedisHealthCheckInstance(), th);
        this.cluster = cluster;
        this.shard = shard;
    }

    public String getCluster() {
        return cluster;
    }

    public String getShard() {
        return shard;
    }
}
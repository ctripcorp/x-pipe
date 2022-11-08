package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

import java.util.Objects;

public class HeteroDelayActionContext extends DelayActionContext {
    private Long shardDbId;
    private boolean isExpired;

    public HeteroDelayActionContext(RedisHealthCheckInstance instance, Long shardId, Long delay) {
        super(instance, delay);
        this.shardDbId = shardId;
    }

    public Long getShardDbId() {
        return shardDbId;
    }

    public String getDelayType() {
        return "hetero";
    }

    public boolean isExpired() {
        return isExpired;
    }

    public void setExpired(boolean expired) {
        isExpired = expired;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        HeteroDelayActionContext context = (HeteroDelayActionContext) o;
        return shardDbId.equals(context.shardDbId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), shardDbId);
    }
}

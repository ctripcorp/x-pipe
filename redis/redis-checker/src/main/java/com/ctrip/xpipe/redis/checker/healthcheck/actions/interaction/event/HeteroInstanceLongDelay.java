package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event;

import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

public class HeteroInstanceLongDelay extends AbstractInstanceEvent {

    private final long srcShardDBId;

    public HeteroInstanceLongDelay(RedisHealthCheckInstance instance, long srcShardDBId) {
        super(instance);
        this.srcShardDBId = srcShardDBId;
    }

    public long getSrcShardDBId() {
        return srcShardDBId;
    }

    @Override
    public String toString() {
        return String.format("%s:%d:%s", getInstance(), srcShardDBId, getClass().getSimpleName());
    }

}

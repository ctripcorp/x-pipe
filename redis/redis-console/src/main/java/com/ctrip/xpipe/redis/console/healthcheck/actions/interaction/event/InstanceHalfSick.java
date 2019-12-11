package com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event;

import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.utils.ObjectUtils;

public class InstanceHalfSick extends AbstractInstanceEvent {

    public InstanceHalfSick(RedisHealthCheckInstance instance) {
        super(instance);
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(instance.getRedisInstanceInfo().getHostPort());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof InstanceHalfSick)) {
            return false;
        }
        InstanceHalfSick other = (InstanceHalfSick) obj;
        return other.instance.getRedisInstanceInfo().getHostPort().equals(this.instance.getRedisInstanceInfo().getHostPort());
    }
}

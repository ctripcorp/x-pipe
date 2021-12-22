package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event;

import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.utils.ObjectUtils;

public class InstanceLongDelay extends AbstractInstanceEvent {

    public InstanceLongDelay(RedisHealthCheckInstance instance) {
        super(instance);
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(instance.getCheckInfo().getHostPort());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof InstanceLongDelay)) {
            return false;
        }
        InstanceLongDelay other = (InstanceLongDelay) obj;
        return other.instance.getCheckInfo().getHostPort().equals(this.instance.getCheckInfo().getHostPort());
    }
}

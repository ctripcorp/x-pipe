package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event;

import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.utils.ObjectUtils;

public class InstanceHalfSick extends AbstractInstanceEvent {

    public InstanceHalfSick(RedisHealthCheckInstance instance) {
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
        if (!(obj instanceof InstanceHalfSick)) {
            return false;
        }
        InstanceHalfSick other = (InstanceHalfSick) obj;
        return other.instance.getCheckInfo().getHostPort().equals(this.instance.getCheckInfo().getHostPort());
    }
}

package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data;

import com.ctrip.xpipe.endpoint.HostPort;

import java.util.Set;

/**
 * @author lishanglin
 * date 2022/7/21
 */
public class UpDownInstances {

    private Set<HostPort> healthyInstances;

    private Set<HostPort> unhealthyInstances;

    public UpDownInstances(Set<HostPort> healthyInstances, Set<HostPort> unhealthyInstances) {
        this.healthyInstances = healthyInstances;
        this.unhealthyInstances = unhealthyInstances;
    }

    public Set<HostPort> getHealthyInstances() {
        return healthyInstances;
    }

    public Set<HostPort> getUnhealthyInstances() {
        return unhealthyInstances;
    }

}

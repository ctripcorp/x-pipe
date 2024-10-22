package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStatusDesc;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/3/17
 */
public interface CheckerService {

    class AllInstanceHealthStatus extends HashMap<HostPort, HealthStatusDesc> {}

    HEALTH_STATE getInstanceStatus(String ip, int port);

    HEALTH_STATE getCrossRegionInstanceStatus(String ip, int port);

    Map<HostPort, HealthStatusDesc> getAllInstanceHealthStatus();

    Map<HostPort, HealthStatusDesc> getAllInstanceCrossRegionHealthStatus();

    Map<HostPort, HealthStatusDesc> getAllClusterInstanceHealthStatus(Set<HostPort> hostPorts);

}

package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import com.ctrip.xpipe.endpoint.HostPort;

import java.util.Map;

/**
 * @author lishanglin
 * date 2021/4/30
 */
public interface HealthStateService {

    HEALTH_STATE getHealthState(HostPort hostPort);

    HealthStatusDesc getHealthStatusDesc(HostPort hostPort);

    Map<HostPort, HEALTH_STATE> getAllCachedState();

    void updateHealthState(Map<HostPort, HEALTH_STATE> redisStates);

    void updateLastMarkHandled(HostPort hostPort, boolean lastMark);

}

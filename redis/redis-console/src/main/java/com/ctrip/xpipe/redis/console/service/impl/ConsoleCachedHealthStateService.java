package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStateService;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStatusDesc;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerMode;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition;
import com.google.common.collect.Maps;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @author lishanglin
 * date 2021/4/30
 */
@Component
@ConsoleServerMode(ConsoleServerModeCondition.SERVER_MODE.CONSOLE)
public class ConsoleCachedHealthStateService implements HealthStateService {

    private Map<HostPort, HEALTH_STATE> cachedRedisStates = Maps.newConcurrentMap();

    @Override
    public HEALTH_STATE getHealthState(HostPort hostPort) {
        return cachedRedisStates.get(hostPort);
    }

    @Override
    public Map<HostPort, HEALTH_STATE> getAllCachedState() {
        return cachedRedisStates;
    }

    @Override
    public void updateHealthState(Map<HostPort, HEALTH_STATE> redisStates) {
        cachedRedisStates.putAll(redisStates);
    }

    @Override
    public HealthStatusDesc getHealthStatusDesc(HostPort hostPort) {
        if (cachedRedisStates.containsKey(hostPort)) {
            return new HealthStatusDesc(hostPort, cachedRedisStates.get(hostPort));
        } else {
            return new HealthStatusDesc(hostPort, HEALTH_STATE.UNKNOWN);
        }
    }

    @Override
    public void updateLastMarkHandled(HostPort hostPort, boolean lastMark) {
        // do nothing
    }
}

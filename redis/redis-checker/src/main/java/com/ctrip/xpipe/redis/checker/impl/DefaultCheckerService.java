package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.CheckerService;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStatusDesc;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import java.util.Map;

/**
 * @author lishanglin
 * date 2021/3/17
 */
public class DefaultCheckerService extends AbstractService implements CheckerService {

    private String host;

    private static final String PATH_GET_HEALTH_STATE = "/api/health/{ip}/{port}";

    private static final String PATH_GET_ALL_INSTANCE_HEALTH_STATUS = "/api/health/check/status/all";

    public DefaultCheckerService(String host) {
        if (host.startsWith("http://")) this.host = host;
        else this.host = "http://" + host;
    }

    @Override
    public HEALTH_STATE getInstanceStatus(String ip, int port) {
        return restTemplate.getForObject(host + PATH_GET_HEALTH_STATE, HEALTH_STATE.class, ip, port);
    }

    @Override
    public Map<HostPort, HealthStatusDesc> getAllInstanceHealthStatus() {
        return restTemplate.getForObject(host + PATH_GET_ALL_INSTANCE_HEALTH_STATUS, AllInstanceHealthStatus.class);
    }
}

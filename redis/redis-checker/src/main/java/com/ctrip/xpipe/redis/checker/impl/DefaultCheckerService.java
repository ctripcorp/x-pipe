package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.redis.checker.CheckerService;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.core.service.AbstractService;

/**
 * @author lishanglin
 * date 2021/3/17
 */
public class DefaultCheckerService extends AbstractService implements CheckerService {

    private String host;

    private static final String PATH_GET_HEALTH_STATE = "/api/health/{ip}/{port}";

    public DefaultCheckerService(String host) {
        if (host.startsWith("http://")) this.host = host;
        else this.host = "http://" + host;
    }

    @Override
    public HEALTH_STATE getInstanceStatus(String ip, int port) {
        return restTemplate.getForObject(host + PATH_GET_HEALTH_STATE, HEALTH_STATE.class, ip, port);
    }

}

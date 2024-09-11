package com.ctrip.xpipe.redis.console.checker.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.console.checker.ConsoleCheckerApiService;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import org.springframework.stereotype.Component;

@Component
public class DefaultConsoleCheckerApiService extends AbstractService implements ConsoleCheckerApiService {

    @Override
    public String getHealthCheckInstance(HostPort checker, String ip, int port) {
        return restTemplate.getForObject(getPath(checker, PATH_HEALTH_CHECK_INSTANCE), String.class, ip, port);
    }

    @Override
    public HEALTH_STATE getHealthStates(HostPort checker, String ip, int port) {
        return restTemplate.getForObject(getPath(checker, PATH_HEALTH_STATUS), HEALTH_STATE.class, ip, port);
    }

    private String getPath(HostPort key, String path) {
        return "http://" + key.getHost() + ":" + key.getPort() + path;
    }


}

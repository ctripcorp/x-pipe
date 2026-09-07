package com.ctrip.xpipe.redis.console.checker.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.console.checker.ConsoleCheckerApiService;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class DefaultConsoleCheckerApiService extends AbstractService implements ConsoleCheckerApiService {

    private static final Logger logger = LoggerFactory.getLogger(DefaultConsoleCheckerApiService.class);

    @Override
    public String getHealthCheckInstance(HostPort checker, String ip, int port) {
        return restTemplate.getForObject(getPath(checker, PATH_HEALTH_CHECK_INSTANCE), String.class, ip, port);
    }

    @Override
    public String getCrossRegionHealthCheckInstance(HostPort checker, String ip, int port) {
        return restTemplate.getForObject(getPath(checker, PATH_CROSS_REGION_HEALTH_CHECK_INSTANCE), String.class, ip, port);
    }

    @Override
    public HEALTH_STATE getHealthStates(HostPort checker, String ip, int port) {
        try {
            return restTemplate.getForObject(getPath(checker, PATH_HEALTH_STATUS), HEALTH_STATE.class, ip, port);
        } catch (Throwable th) {
            logger.info("[getHealthStates][fail] checker={}, {}({}), cause={}", checker, ip, port, th.getMessage());
            throw th;
        }
    }

    @Override
    public HEALTH_STATE getCrossRegionHealthStates(HostPort checker, String ip, int port) {
        try {
            return restTemplate.getForObject(getPath(checker, PATH_CROSS_REGION_HEALTH_STATUS), HEALTH_STATE.class, ip, port);
        } catch (Throwable th) {
            logger.info("[getCrossRegionHealthStates][fail] checker={}, {}({}), cause={}", checker, ip, port, th.getMessage());
            throw th;
        }
    }

    private String getPath(HostPort key, String path) {
        return "http://" + key.getHost() + ":" + key.getPort() + path;
    }


}

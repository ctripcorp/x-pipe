package com.ctrip.xpipe.redis.console.checker;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.controller.result.ActionContextRetMessage;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo.InfoActionContext;
import com.ctrip.xpipe.redis.core.service.AbstractService;

import java.util.Collections;
import java.util.Map;

/**
 * @author Slight
 * <p>
 * Jun 04, 2021 2:52 PM
 */
public class DefaultConsoleCheckerService extends AbstractService implements ConsoleCheckerService {

    private String address;

    private final String allRedisInfosUrl;

    public DefaultConsoleCheckerService(HostPort hostPort) {
        this.address = hostPort.toString();
        if(!this.address.startsWith("http://")){
            this.address = "http://" + this.address;
        }
        allRedisInfosUrl = String.format("%s/api/health/redis/info/all", this.address);
    }

    public Map<HostPort, ActionContextRetMessage<Map<String, String>>> getAllLocalRedisInfos() {
        try {
            return restTemplate.getForObject(allRedisInfosUrl, InfoActionContext.ResultMap.class);
        } catch (Throwable t) {
            return Collections.emptyMap();
        }
    }
}

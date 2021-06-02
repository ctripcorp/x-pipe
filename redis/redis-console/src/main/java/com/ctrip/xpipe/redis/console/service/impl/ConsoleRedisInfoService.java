package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo.InfoActionContext;
import com.ctrip.xpipe.redis.console.checker.CheckerManager;
import com.ctrip.xpipe.redis.console.service.RedisInfoService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

/**
 * @author Slight
 * <p>
 * Jun 01, 2021 5:35 PM
 */
public class ConsoleRedisInfoService implements RedisInfoService {

    @Autowired
    private CheckerManager checkerManager;

    @Override
    public InfoActionContext getInfoByHostPort(HostPort hostPort) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<HostPort, InfoActionContext> getAllInfos() {
        throw new UnsupportedOperationException();
    }
}

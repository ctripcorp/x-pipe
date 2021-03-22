package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingService;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * @author lishanglin
 * date 2021/3/17
 */
public class ConsoleCachedPingService implements PingService {

    Map<HostPort, Boolean> redisAlives = Maps.newConcurrentMap();

    @Override
    public void updateRedisAlives(Map<HostPort, Boolean> redisAlives) {
        this.redisAlives.putAll(redisAlives);
    }

    @Override
    public boolean isRedisAlive(HostPort hostPort) {
        return redisAlives.containsKey(hostPort) && redisAlives.get(hostPort);
    }

    @Override
    public Map<HostPort, Boolean> getAllRedisAlives() {
        return redisAlives;
    }
}

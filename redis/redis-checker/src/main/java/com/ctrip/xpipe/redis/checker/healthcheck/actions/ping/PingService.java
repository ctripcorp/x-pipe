package com.ctrip.xpipe.redis.checker.healthcheck.actions.ping;

import com.ctrip.xpipe.endpoint.HostPort;

import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * Sep 03, 2018
 */
public interface PingService {

    void updateRedisAlives(Map<HostPort, Boolean> redisAlives);

    boolean isRedisAlive(HostPort hostPort);

    Map<HostPort, Boolean> getAllRedisAlives();

}

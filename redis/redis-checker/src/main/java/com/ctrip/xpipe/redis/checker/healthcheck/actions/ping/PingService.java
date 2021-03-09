package com.ctrip.xpipe.redis.checker.healthcheck.actions.ping;

import com.ctrip.xpipe.endpoint.HostPort;

/**
 * @author chen.zhu
 * <p>
 * Sep 03, 2018
 */
public interface PingService {
    boolean isRedisAlive(HostPort hostPort);
}

package com.ctrip.xpipe.redis.console.healthcheck.actions.ping;

import com.ctrip.xpipe.endpoint.HostPort;

/**
 * @author chen.zhu
 * <p>
 * Sep 03, 2018
 */
public interface PingService {
    boolean isRedisAlive(HostPort hostPort);
}

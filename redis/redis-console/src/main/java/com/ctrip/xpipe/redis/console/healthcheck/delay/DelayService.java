package com.ctrip.xpipe.redis.console.healthcheck.delay;

import com.ctrip.xpipe.endpoint.HostPort;

/**
 * @author chen.zhu
 * <p>
 * Sep 03, 2018
 */
public interface DelayService {

    long getDelay(HostPort hostPort);
}

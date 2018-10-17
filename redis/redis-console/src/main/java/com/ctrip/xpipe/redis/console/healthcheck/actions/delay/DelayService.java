package com.ctrip.xpipe.redis.console.healthcheck.actions.delay;

import com.ctrip.xpipe.endpoint.HostPort;

/**
 * @author chen.zhu
 * <p>
 * Sep 03, 2018
 */
public interface DelayService {

    long getDelay(HostPort hostPort);
}

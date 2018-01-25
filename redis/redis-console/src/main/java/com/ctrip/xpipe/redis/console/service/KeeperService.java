package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.endpoint.HostPort;

/**
 * @author chen.zhu
 * <p>
 * Jan 23, 2018
 */
public interface KeeperService {
    boolean isKeeper(HostPort hostPort);
}

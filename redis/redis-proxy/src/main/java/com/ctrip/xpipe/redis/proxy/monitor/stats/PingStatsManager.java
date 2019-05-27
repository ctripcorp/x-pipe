package com.ctrip.xpipe.redis.proxy.monitor.stats;

import com.ctrip.xpipe.proxy.ProxyEndpoint;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 31, 2018
 */
public interface PingStatsManager {

    List<PingStats> getAllPingStats();

    PingStats create(ProxyEndpoint endpoint);
}

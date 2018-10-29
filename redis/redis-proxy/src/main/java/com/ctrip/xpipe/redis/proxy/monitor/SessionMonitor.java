package com.ctrip.xpipe.redis.proxy.monitor;

import com.ctrip.xpipe.redis.proxy.monitor.session.SessionStats;

/**
 * @author chen.zhu
 * <p>
 * Oct 29, 2018
 */
public interface SessionMonitor {

    SessionStats getSessionStats();
}

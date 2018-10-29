package com.ctrip.xpipe.redis.proxy.monitor.session;

import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.monitor.SessionMonitor;
import com.ctrip.xpipe.redis.proxy.resource.ResourceManager;

/**
 * @author chen.zhu
 * <p>
 * Oct 29, 2018
 */
public class DefaultSessionMonitor implements SessionMonitor {

    private ResourceManager resourceManager;

    private Session session;

    private SessionStats sessionStats;

    public DefaultSessionMonitor(ResourceManager resourceManager, Session session) {
        this.resourceManager = resourceManager;
        this.sessionStats = new DefaultSessionStats(resourceManager.getGlobalSharedScheduled());
        this.session = session;
    }

    @Override
    public SessionStats getSessionStats() {
        return sessionStats;
    }
}

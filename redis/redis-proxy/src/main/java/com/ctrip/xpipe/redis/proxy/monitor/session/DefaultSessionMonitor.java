package com.ctrip.xpipe.redis.proxy.monitor.session;

import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.monitor.SessionMonitor;
import com.ctrip.xpipe.redis.proxy.monitor.stats.SocketStats;
import com.ctrip.xpipe.redis.proxy.resource.ResourceManager;

/**
 * @author chen.zhu
 * <p>
 * Oct 29, 2018
 */
public class DefaultSessionMonitor extends AbstractStartStoppable implements SessionMonitor {

    private ResourceManager resourceManager;

    private Session session;

    private SessionStats sessionStats;

    private OutboundBufferMonitor outboundBufferMonitor;

    public DefaultSessionMonitor(ResourceManager resourceManager, Session session) {
        this.resourceManager = resourceManager;
        this.sessionStats = new DefaultSessionStats(resourceManager.getGlobalSharedScheduled());
        this.outboundBufferMonitor = new DefaultOutboundBufferMonitor(session);
        this.session = session;
    }

    @Override
    public SessionStats getSessionStats() {
        return sessionStats;
    }

    @Override
    public OutboundBufferMonitor getOutboundBufferMonitor() {
        return outboundBufferMonitor;
    }

    @Override
    public SocketStats getSocketStats() {
        return null;
    }

    @Override
    protected void doStart() {
        try {
            sessionStats.start();
        } catch (Exception e) {
            logger.error("[doStart]", e);
        }
    }

    @Override
    protected void doStop() {
        try {
            sessionStats.stop();
        } catch (Exception e) {
            logger.error("[doStart]", e);
        }
    }
}

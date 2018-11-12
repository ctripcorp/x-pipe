package com.ctrip.xpipe.redis.proxy.monitor.session;

import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.monitor.SessionMonitor;
import com.ctrip.xpipe.redis.proxy.monitor.stats.SocketStats;
import com.ctrip.xpipe.redis.proxy.monitor.stats.impl.DefaultSocketStats;
import com.ctrip.xpipe.redis.proxy.resource.ResourceManager;

/**
 * @author chen.zhu
 * <p>
 * Oct 29, 2018
 */
public class DefaultSessionMonitor extends AbstractStartStoppable implements SessionMonitor {

    private SessionStats sessionStats;

    private OutboundBufferMonitor outboundBufferMonitor;

    private SocketStats socketStats;

    public static final String SESSION_MONITOR_SHOULD_START = "proxy.session.monitor.start";

    private static final boolean shouldStart = Boolean.parseBoolean(System.getProperty(SESSION_MONITOR_SHOULD_START, "true"));

    public DefaultSessionMonitor(ResourceManager resourceManager, Session session) {
        this.sessionStats = new DefaultSessionStats(resourceManager.getGlobalSharedScheduled());
        this.outboundBufferMonitor = new DefaultOutboundBufferMonitor(session);
        this.socketStats = new DefaultSocketStats(resourceManager.getGlobalSharedScheduled(), session);
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
        return socketStats;
    }

    @Override
    protected void doStart() throws Exception {
        if(shouldStart) {
            sessionStats.start();
            socketStats.start();
        }
    }

    @Override
    protected void doStop() throws Exception {
        if(shouldStart) {
            sessionStats.stop();
            socketStats.stop();
        }
    }
}

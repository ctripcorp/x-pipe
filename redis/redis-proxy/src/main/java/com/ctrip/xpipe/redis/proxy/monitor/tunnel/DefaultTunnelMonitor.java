package com.ctrip.xpipe.redis.proxy.monitor.tunnel;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.monitor.SessionMonitor;
import com.ctrip.xpipe.redis.proxy.monitor.TunnelMonitor;
import com.ctrip.xpipe.redis.proxy.monitor.session.DefaultSessionMonitor;
import com.ctrip.xpipe.redis.proxy.monitor.session.SessionStats;
import com.ctrip.xpipe.redis.proxy.resource.ResourceManager;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Oct 29, 2018
 */
public class DefaultTunnelMonitor extends AbstractStartStoppable implements TunnelMonitor {

    private ResourceManager resourceManager;

    private Tunnel tunnel;

    private SessionMonitor frontendSessionMonitor;

    private SessionMonitor backendSessionMonitor;

    private ScheduledFuture future;

    public DefaultTunnelMonitor(ResourceManager resourceManager, Tunnel tunnel) {
        this.resourceManager = resourceManager;
        this.tunnel = tunnel;
        frontendSessionMonitor = new DefaultSessionMonitor(resourceManager, tunnel.frontend());
        backendSessionMonitor = new DefaultSessionMonitor(resourceManager, tunnel.backend());

    }

    @Override
    public SessionMonitor getFrontendSessionMonitor() {
        return frontendSessionMonitor;
    }

    @Override
    public SessionMonitor getBackendSessionMonitor() {
        return backendSessionMonitor;
    }

    @Override
    protected void doStart() throws Exception {
        frontendSessionMonitor.getSessionStats().start();
        backendSessionMonitor.getSessionStats().start();
        monitorSession();
    }

    @Override
    protected void doStop() throws Exception {
        if(future != null) {
            future.cancel(true);
        }
        frontendSessionMonitor.getSessionStats().stop();
        backendSessionMonitor.getSessionStats().stop();
    }

    private void monitorSession() {
        int interval = getCheckInterval();
        ScheduledExecutorService scheduled = resourceManager.getGlobalSharedScheduled();
        future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                boolean frontend = checkSessionStats(frontendSessionMonitor.getSessionStats());
                boolean backend = checkSessionStats(backendSessionMonitor.getSessionStats());
                int threshold = resourceManager.getProxyConfig().getSessionIdleTimeMilli();
                if(!frontend) {
                    logger.warn("[monitorSession] close frontend, because no input/output for {} milli", threshold);
                    EventMonitor.DEFAULT.logAlertEvent("[FRONTEND][IDLE CLOSE]" + tunnel.toString());
                    tunnel.frontend().release();
                }
                if(!backend) {
                    logger.warn("[monitorSession] close backend, because no input/output for {} milli", threshold);
                    EventMonitor.DEFAULT.logAlertEvent("[BACKEND][IDLE CLOSE]" + tunnel.toString());
                    tunnel.backend().release();
                }
            }
        }, interval, interval, TimeUnit.MILLISECONDS);
    }

    protected int getCheckInterval() {
        return 10 * 1000;
    }

    private boolean checkSessionStats(SessionStats stats) {
        long delta = System.currentTimeMillis() - stats.lastUpdateTime();
        long idleTime = resourceManager.getProxyConfig().getSessionIdleTimeMilli();
        return delta < idleTime;
    }
}

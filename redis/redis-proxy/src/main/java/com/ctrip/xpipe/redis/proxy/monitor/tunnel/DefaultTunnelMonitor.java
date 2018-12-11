package com.ctrip.xpipe.redis.proxy.monitor.tunnel;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.monitor.SessionMonitor;
import com.ctrip.xpipe.redis.proxy.monitor.TunnelMonitor;
import com.ctrip.xpipe.redis.proxy.monitor.TunnelRecorder;
import com.ctrip.xpipe.redis.proxy.monitor.session.DefaultSessionMonitor;
import com.ctrip.xpipe.redis.proxy.monitor.stats.TunnelStats;
import com.ctrip.xpipe.redis.proxy.monitor.stats.impl.DefaultTunnelStats;
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

    private SessionMonitor frontendSessionMonitor;

    private SessionMonitor backendSessionMonitor;

    private TunnelStats tunnelStats;

    private TunnelRecorder recorder;

    private Tunnel tunnel;

    private ResourceManager resourceManager;

    private ScheduledFuture future;

    public DefaultTunnelMonitor(ResourceManager resourceManager, Tunnel tunnel, TunnelRecorder recorder) {
        this.tunnel = tunnel;
        this.resourceManager = resourceManager;
        this.recorder = recorder;
        frontendSessionMonitor = new DefaultSessionMonitor(resourceManager, tunnel.frontend());
        backendSessionMonitor = new DefaultSessionMonitor(resourceManager, tunnel.backend());
        tunnelStats = new DefaultTunnelStats(tunnel);

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
    public TunnelStats getTunnelStats() {
        return tunnelStats;
    }

    @Override
    public void record(Tunnel tunnel) {
        if(resourceManager.getProxyConfig().startMonitor()) {
            recorder.record(tunnel);
        }
    }

    @Override
    protected void doStart() throws Exception {
        if(resourceManager.getProxyConfig().startMonitor()) {
            frontendSessionMonitor.start();
            backendSessionMonitor.start();
            future = resourceManager.getGlobalSharedScheduled().scheduleWithFixedDelay(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() {
                    record(tunnel);
                }
            }, 5000, 1000, TimeUnit.MILLISECONDS);
        }

    }

    @Override
    protected void doStop() throws Exception {
        if(resourceManager.getProxyConfig().startMonitor()) {
            frontendSessionMonitor.stop();
            backendSessionMonitor.stop();
            if(future != null) {
                future.cancel(true);
            }
        }
    }

}

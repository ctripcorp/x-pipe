package com.ctrip.xpipe.redis.proxy.monitor.tunnel;

import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.monitor.SessionMonitor;
import com.ctrip.xpipe.redis.proxy.monitor.TunnelMonitor;
import com.ctrip.xpipe.redis.proxy.monitor.TunnelRecorder;
import com.ctrip.xpipe.redis.proxy.monitor.session.DefaultSessionMonitor;
import com.ctrip.xpipe.redis.proxy.monitor.stats.TunnelStats;
import com.ctrip.xpipe.redis.proxy.monitor.stats.impl.DefaultTunnelStats;
import com.ctrip.xpipe.redis.proxy.resource.ResourceManager;


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

    public DefaultTunnelMonitor(ResourceManager resourceManager, Tunnel tunnel, TunnelRecorder recorder) {
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
        recorder.record(tunnel);
    }

    @Override
    protected void doStart() throws Exception {
        frontendSessionMonitor.start();
        backendSessionMonitor.start();
    }

    @Override
    protected void doStop() throws Exception {
        frontendSessionMonitor.stop();
        backendSessionMonitor.stop();
    }

}

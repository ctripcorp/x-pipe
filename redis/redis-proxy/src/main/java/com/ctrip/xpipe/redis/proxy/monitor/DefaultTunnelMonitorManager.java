package com.ctrip.xpipe.redis.proxy.monitor;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.monitor.tunnel.DefaultTunnelMonitor;
import com.ctrip.xpipe.redis.proxy.monitor.tunnel.DefaultTunnelRecorder;
import com.ctrip.xpipe.redis.proxy.resource.ResourceManager;
import com.ctrip.xpipe.redis.proxy.tunnel.state.TunnelClosed;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 29, 2018
 */
@Component
public class DefaultTunnelMonitorManager implements TunnelMonitorManager {

    private static final Logger logger = LoggerFactory.getLogger(DefaultTunnelMonitorManager.class);

    private Map<Tunnel, TunnelMonitor> tunnelMonitors = new ConcurrentHashMap<>();

    @Autowired
    private ResourceManager resourceManager;

    private TunnelRecorder recorder = new DefaultTunnelRecorder();

    private ScheduledExecutorService scheduled;

    private ScheduledFuture future;

    private List<Tunnel> mayLeakTunnels;

    public DefaultTunnelMonitorManager() {
    }

    public DefaultTunnelMonitorManager(ResourceManager resourceManager) {
        this.resourceManager = resourceManager;
    }

    @PostConstruct
    public void postConstruct() {
        this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("CheckerMetaLoader"));
        this.mayLeakTunnels = new ArrayList<>();
        this.future = this.scheduled.scheduleWithFixedDelay(this::clean, 60, 60, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void preDestroy() {
        if (null != future) {
            future.cancel(false);
            future = null;
        }
        this.scheduled.shutdown();
    }

    @Override
    public TunnelMonitor getOrCreate(Tunnel tunnel) {
        return MapUtils.getOrCreate(tunnelMonitors, tunnel, new ObjectFactory<TunnelMonitor>() {
            @Override
            public TunnelMonitor create() {
                return new DefaultTunnelMonitor(resourceManager, tunnel, recorder);
            }
        });
    }

    @Override
    public void remove(Tunnel tunnel) {
        TunnelMonitor monitor = tunnelMonitors.remove(tunnel);
        try {
            if(monitor != null) {
                monitor.stop();
            }
        } catch (Exception e) {
            logger.error("[stop tunnel-monitor]", e);
        }
    }

    @Override
    public Set<Tunnel> getAllTunnels() {
        return tunnelMonitors.keySet();
    }

    private void clean() {
        // remove tunnel found in the last run
        for (Tunnel tunnel: mayLeakTunnels) {
            if (!(tunnel.getState() instanceof TunnelClosed)) continue;
            if (!this.tunnelMonitors.containsKey(tunnel)) continue;
            logger.info("[clean][leak tunnel] {}", tunnel.identity());
            remove(tunnel);
        }

        List<Tunnel> localTunnels = new ArrayList<>();
        for (Tunnel tunnel: tunnelMonitors.keySet()) {
            if (tunnel.getState() instanceof TunnelClosed) {
                localTunnels.add(tunnel);
            }
        }

        this.mayLeakTunnels = localTunnels;
    }

    @VisibleForTesting
    protected Map<Tunnel, TunnelMonitor> getTunnelMonitors() {
        return tunnelMonitors;
    }
}

package com.ctrip.xpipe.redis.proxy.tunnel;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.redis.proxy.monitor.TunnelMonitorManager;
import com.ctrip.xpipe.redis.proxy.resource.ResourceManager;
import com.ctrip.xpipe.redis.proxy.tunnel.state.TunnelClosed;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.THREAD_POOL_TIME_OUT;

/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */
@Component
public class DefaultTunnelManager implements TunnelManager {

    private static final Logger logger = LoggerFactory.getLogger(DefaultTunnelManager.class);

    @Autowired
    private ProxyConfig config;

    @Autowired
    private ResourceManager proxyResourceManager;

    @Autowired
    private TunnelMonitorManager tunnelMonitorManager;

    private Map<Channel, Tunnel> cache = Maps.newConcurrentMap();

    private ScheduledFuture future;

    @PostConstruct
    public void cleaner() {
        ScheduledExecutorService scheduled = MoreExecutors.getExitingScheduledExecutorService(
                new ScheduledThreadPoolExecutor(1, XpipeThreadFactory.create("DefaultTunnelManager")),
                THREAD_POOL_TIME_OUT, TimeUnit.SECONDS);

        future = scheduled.scheduleWithFixedDelay(
                new AbstractExceptionLogTask() {
                    @Override
                    protected void doRun() throws Exception {
                        doClean();
                    }
                }, 10, 10, TimeUnit.MINUTES);
    }

    @PreDestroy
    public void preDestroy() throws Exception {
        release();
    }

    @VisibleForTesting
    protected void doClean() {
        Set<Channel> keys = Sets.newHashSet(cache.keySet());
        for(Channel channel : keys) {
            if (!channel.isActive()) {
                Tunnel tunnel = cache.remove(channel);
                try {
                    tunnel.release();
                } catch (Exception e) {
                    logger.error("[cleaner] tunnel release error: ", e);
                }
            } else {

                Tunnel tunnel = cache.get(channel);
                logger.info("[doClean] check tunnel, {}", tunnel.getTunnelMeta());
                if (tunnel.getState().equals(new TunnelClosed(null))) {
                    cache.remove(channel);
                }
            }
        }
    }

    @Override
    public Tunnel create(Channel frontendChannel, ProxyConnectProtocol protocol) {
        Tunnel tunnel = MapUtils.getOrCreate(cache, frontendChannel, new ObjectFactory<Tunnel>() {
            @Override
            public Tunnel create() {
                return new DefaultTunnel(frontendChannel, protocol, config, proxyResourceManager, tunnelMonitorManager);
            }
        });
        initAndStart(tunnel);
        tunnel.addObserver(this);

        return tunnel;
    }

    private void initAndStart(Tunnel tunnel) {
        try {
            LifecycleHelper.initializeIfPossible(tunnel);
        } catch (Exception e) {
            logger.error("[initAndStart][init] ", e);
        }
        try {
            LifecycleHelper.startIfPossible(tunnel);
        } catch (Exception e) {
            logger.error("[initAndStart][start] ", e);
        }
    }

    @Override
    public void remove(Channel frontendChannel) {
        Tunnel tunnel = cache.remove(frontendChannel);
        try {
            LifecycleHelper.stopIfPossible(tunnel);
            LifecycleHelper.disposeIfPossible(tunnel);
        } catch (Exception e) {
            logger.error("[remove] error dispose Tunnel: {}", tunnel.getTunnelMeta(), e);
        }
    }

    @Override
    public List<Tunnel> tunnels() {
        return Lists.newArrayList(cache.values());
    }

    @Override
    public Tunnel getById(String id) {
        for(Tunnel tunnel : tunnels()) {
            if(tunnel.identity().equals(id)) {
                return tunnel;
            }
        }
        return null;
    }

    @Override
    public void release() throws Exception {
        if(future != null) {
            future.cancel(true);
        }
        for(Map.Entry<Channel, Tunnel> entry : cache.entrySet()) {
            Tunnel tunnel = entry.getValue();
            if(tunnel != null) {
                tunnel.release();
            }
            entry.getKey().close();
        }
    }

    // observer for tunnel change
    @Override
    public void update(Object args, Observable observable) {
        if(!(observable instanceof Tunnel)) {
            logger.error("[update] should observe tunnel only, not {}", observable.getClass().getName());
            return;
        }
        DefaultTunnel tunnel = (DefaultTunnel) observable;

        // deal with TunnelStateChangeEvent Only for current
        if(!(args instanceof TunnelStateChangeEvent)) {
            return;
        }
        TunnelStateChangeEvent event = (TunnelStateChangeEvent) args;

        if(event.getCurrent() instanceof TunnelClosed) {
            logger.info("[update] tunnel closed, remove from tunnel manager");
            remove(tunnel.frontendChannel());

        }
    }

    // Unit Test

    @VisibleForTesting
    public DefaultTunnelManager setConfig(ProxyConfig config) {
        this.config = config;
        return this;
    }

    @VisibleForTesting
    public DefaultTunnelManager setProxyResourceManager(ResourceManager manager) {
        this.proxyResourceManager = manager;
        return this;
    }

    public DefaultTunnelManager setTunnelMonitorManager(TunnelMonitorManager tunnelMonitorManager) {
        this.tunnelMonitorManager = tunnelMonitorManager;
        return this;
    }
}

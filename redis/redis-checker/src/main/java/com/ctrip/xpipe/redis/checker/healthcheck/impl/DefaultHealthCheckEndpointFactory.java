package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.framework.xpipe.redis.ProxyChecker;
import com.ctrip.framework.xpipe.redis.ProxyRegistry;
import com.ctrip.framework.xpipe.redis.instrument.AgentMain;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.RouteChooser;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategyFactory;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ctrip.xpipe.redis.checker.config.impl.CheckConfigBean.KEY_PROXY_CHECK_INTERVAL;

/**
 * @author chen.zhu
 * <p>
 * Sep 03, 2018
 */
@Component
public class DefaultHealthCheckEndpointFactory implements HealthCheckEndpointFactory {

    static final Logger logger = LoggerFactory.getLogger(DefaultHealthCheckEndpointFactory.class);

    private final String PROXY_UP_EVENT = "proxy.client.up";

    private final String PROXY_DOWN_EVENT = "proxy.client.down";

    private ConcurrentMap<HostPort, Endpoint> map = Maps.newConcurrentMap();

    private ProxyChecker proxyChecker;

    private CheckerConfig config;

    private MetaCache metaCache;

    private RouteChooser routeChooser;

    private AtomicBoolean chooserInited = new AtomicBoolean(false);

    @Autowired
    public DefaultHealthCheckEndpointFactory(ProxyChecker proxyChecker, CheckerConfig config, MetaCache metaCache,
                                             RouteChooseStrategyFactory routeChooseStrategyFactory) {
        this.proxyChecker = proxyChecker;
        this.config = config;
        this.metaCache = metaCache;

        this.routeChooser = new DefaultRouteChooser(routeChooseStrategyFactory.create(RouteChooseStrategyFactory.RouteStrategyType.CRC32_HASH));
    }

    private synchronized void initRoutes() {
        if (chooserInited.get()) return;

        List<RouteMeta> allRoutes = metaCache.getCurrentDcConsoleRoutes();
        if (null != allRoutes) {
            this.routeChooser.updateRoutes(metaCache.getCurrentDcConsoleRoutes());
            chooserInited.set(true);
        }
    }

    @Override
    public synchronized void updateRoutes() {
        logger.info("[DefaultHealthCheckEndpointFactory][updateRoutes]");
        List<RouteMeta> allRoutes = metaCache.getCurrentDcConsoleRoutes();
        if(allRoutes == null) return;

        this.routeChooser.updateRoutes(allRoutes);
        map.forEach((hostPort, endpoint) -> {
            try {
                registerProxy(hostPort);
            } catch (Throwable th) {
                logger.info("[updateRoutes][{}] fail", hostPort, th);
            }
        });
    }

    private void registerProxy(HostPort hostPort) {
        XpipeMetaManager.MetaDesc metaDesc = metaCache.findMetaDesc(hostPort);
        if (null == metaDesc) {
            logger.info("[registerProxy][{}] meta not found", hostPort);
            return;
        }

        if (!chooserInited.get()) {
            initRoutes();
        }
        RouteMeta route = routeChooser.chooseRoute(metaDesc.getDcId(), metaDesc.getClusterMeta());
        if (null != route) {
            String proxyProtocol = getProxyProtocol(route);
            logger.info("[registerProxy][{}] {}", hostPort, proxyProtocol);
            ProxyRegistry.registerProxy(hostPort.getHost(), hostPort.getPort(), proxyProtocol);
        } else {
            logger.info("[registerProxy][route not found][{}] unregister", hostPort);
            ProxyRegistry.unregisterProxy(hostPort.getHost(), hostPort.getPort());
        }
    }

    @PostConstruct
    public void postConstruct() {
        ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);
        scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                if (!AgentMain.isProxyJarReady()) {
                    logger.debug("[initEndpointChecker][skip] ProxyJar not ready");
                    return;
                }

                initEndpointChecker();
                scheduled.shutdown();
            }
        }, 0, 2, TimeUnit.SECONDS);
    }

    public void initEndpointChecker() {
        logger.info("[initEndpointChecker][begin]");
        ProxyRegistry.setChecker(proxyChecker::check, proxyChecker::getRetryUpTimes, proxyChecker::getRetryDownTimes);
        ProxyRegistry.startCheck();
        ProxyRegistry.onProxyUp(proxyInetSocketAddress ->  {
            logger.info("[proxy-client][up] {}", proxyInetSocketAddress.toString());
            EventMonitor.DEFAULT.logEvent(PROXY_UP_EVENT, proxyInetSocketAddress.toString());
        });
        ProxyRegistry.onProxyDown(proxyInetSocketAddress ->  {
            logger.info("[proxy-client][down] {}", proxyInetSocketAddress.toString());
            EventMonitor.DEFAULT.logEvent(PROXY_DOWN_EVENT, proxyInetSocketAddress.toString());
        });
        config.register(Lists.newArrayList(KEY_PROXY_CHECK_INTERVAL), (key, oldValue, newValue) -> {
            switch (key) {
                case KEY_PROXY_CHECK_INTERVAL:
                    int interval = Integer.parseInt(newValue);
                    if(interval > 0) {
                        ProxyRegistry.setCheckInterval(interval);
                    }
                    break;
            }
        });
        logger.info("[initEndpointChecker][success]");
    }

    @Override
    public Endpoint getOrCreateEndpoint(RedisMeta redisMeta) {
        HostPort hostPort = new HostPort(redisMeta.getIp(), redisMeta.getPort());
        return getOrCreateEndpoint(hostPort);
    }

    @Override
    public void remove(HostPort hostPort) {
        logger.info("[ProxyRegistry] unregisterProxy {}:{}", hostPort.getHost(),hostPort.getPort());
        map.remove(hostPort);
        ProxyRegistry.unregisterProxy(hostPort.getHost(), hostPort.getPort());
    }

    @Override
    public Endpoint getOrCreateEndpoint(HostPort hostPort) {
        Endpoint endpoint = map.get(hostPort);
        if(endpoint != null) {
            return endpoint;
        }

        registerProxy(hostPort);
        endpoint = new DefaultEndPoint(hostPort.getHost(), hostPort.getPort());
        map.put(hostPort, endpoint);
        return endpoint;
    }

    private String getProxyProtocol(RouteMeta route) {
        return String.format("%s %s %s TCP", ProxyConnectProtocol.KEY_WORD, PROXY_OPTION.ROUTE, route.getRouteInfo());
    }

    @VisibleForTesting
    public MetaCache getMetaCache() {
        return this.metaCache;
    }

    @VisibleForTesting
    public DefaultHealthCheckEndpointFactory setMetaCache(MetaCache metaCache) {
        this.metaCache = metaCache;
        return this;
    }

}

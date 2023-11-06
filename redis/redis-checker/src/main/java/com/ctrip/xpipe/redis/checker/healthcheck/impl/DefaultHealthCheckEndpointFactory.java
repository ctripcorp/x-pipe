package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.framework.xpipe.redis.ProxyChecker;
import com.ctrip.framework.xpipe.redis.ProxyRegistry;
import com.ctrip.framework.xpipe.redis.instrument.AgentMain;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static com.ctrip.xpipe.redis.checker.config.CheckerConfig.KEY_PROXY_CHECK_INTERVAL;

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

    @Autowired
    ProxyChecker proxyChecker;

    @Autowired
    CheckerConfig config;

    @Autowired
    private MetaCache metaCache;

    Map<String, List<RouteMeta>> routes;


    public synchronized void initRoutes() {
        if(this.routes != null) {
            return;
        }
        updateRoutes();
        if(this.routes == null) {
            this.routes = Maps.newConcurrentMap();
        }
    }

    @Override
    public synchronized void updateRoutes() {
        logger.info("[DefaultHealthCheckEndpointFactory][updateRoutes]");
        List<RouteMeta> allRoutes = metaCache.getRoutes();
        if(allRoutes == null || allRoutes.size() == 0) return;

        ConcurrentHashMap<String, List<RouteMeta>> newRoutes = new ConcurrentHashMap<>();
        allRoutes.forEach(routeMeta -> {
            if(routeMeta.getIsPublic()) {
                String dst = routeMeta.getDstDc();
                List<RouteMeta> list = MapUtils.getOrCreate(newRoutes, dst, new ObjectFactory<List<RouteMeta>>() {
                    @Override
                    public List<RouteMeta> create() {
                        return new CopyOnWriteArrayList();
                    }
                });
                list.add(routeMeta);
            }
        });
        routes = newRoutes;
        map.forEach((hostPort, endpoint) -> {
            registerProxy(hostPort);
        });
    }

    public RouteMeta selectRoute(List<RouteMeta> routes, HostPort hostPort) {
        int hash = hostPort.hashCode();
        if(hash == Integer.MIN_VALUE){
            return routes.get(0);
        }
        return routes.get(Math.abs(hash) % routes.size());
    }

    private void registerProxy(HostPort hostPort) {
        String dst;
        try {
            dst = metaCache.getDc(hostPort);
        } catch (IllegalStateException notFound) {
            logger.info("[registerProxy] not found {} {}", hostPort, notFound);
            return;
        }
        if(routes == null) {
            initRoutes();
        }
        List<RouteMeta> list = routes.get(dst);

        if(list != null && !list.isEmpty()) {
            RouteMeta route = selectRoute(list, hostPort);
            logger.info("register proxy: {}:{} {}", hostPort.getHost(), hostPort.getPort(), getProxyProtocol(route));
            ProxyRegistry.registerProxy(hostPort.getHost(), hostPort.getPort(), getProxyProtocol(route));
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
        ProxyRegistry.unregisterProxy(hostPort.getHost(), hostPort.getPort());
        map.remove(hostPort);
    }

    @Override
    public Endpoint getOrCreateEndpoint(HostPort hostPort) {
        Endpoint endpoint = map.get(hostPort);
        if(endpoint != null) {
            return endpoint;
        }
        endpoint = new DefaultEndPoint(hostPort.getHost(), hostPort.getPort());
        map.put(hostPort, endpoint);
        registerProxy(hostPort);
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

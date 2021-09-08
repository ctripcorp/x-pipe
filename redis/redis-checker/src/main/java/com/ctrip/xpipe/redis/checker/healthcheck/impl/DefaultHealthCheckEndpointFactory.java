package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.framework.xpipe.redis.ProxyChecker;
import com.ctrip.framework.xpipe.redis.ProxyRegistry;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.proxy.ProxyEnabledEndpoint;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.Route;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.command.ProxyPingCommand;
import com.ctrip.xpipe.redis.core.proxy.command.entity.ProxyPongEntity;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static com.ctrip.xpipe.redis.checker.resource.Resource.KEYED_NETTY_CLIENT_POOL;
import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

/**
 * @author chen.zhu
 * <p>
 * Sep 03, 2018
 */
@Component
public class DefaultHealthCheckEndpointFactory implements HealthCheckEndpointFactory {

    private ConcurrentMap<HostPort, Endpoint> map = Maps.newConcurrentMap();
    
    @Autowired
    private MetaCache metaCache;
    
    Map<String, List<RouteMeta>> routes = new ConcurrentHashMap<>();
    
    @Override
    public void updateRoutes() {
        List<RouteMeta> allRoutes = metaCache.getRoutes();
        if(allRoutes == null || allRoutes.size() == 0) return;
        ConcurrentHashMap<String, List<RouteMeta>> newRoutes = new ConcurrentHashMap<>();
        allRoutes.forEach(routeMeta -> {
            String dst = routeMeta.getDstDc();
            List<RouteMeta> list =  MapUtils.getOrCreate(newRoutes,  dst, new ObjectFactory<List<RouteMeta>>() {
                @Override
                public List<RouteMeta> create() {
                    return new CopyOnWriteArrayList();
                }
            });
            list.add(routeMeta);
        });
        routes = newRoutes;
        map.forEach((hostPort, endpoint) -> {
            registerProxy(hostPort);
        });
    }

    RouteMeta selectRoute(List<RouteMeta> routes, HostPort hostPort) {
        return routes.get(hostPort.hashCode() % routes.size());
    }

    void registerProxy(HostPort hostPort) {
        String dst = metaCache.getDc(hostPort);
        List<RouteMeta> list = routes.get(dst);
        
        if(list != null && list.size() != 0) {
            RouteMeta route = selectRoute(list, hostPort);
            ProxyRegistry.registerProxy(hostPort.getHost(), hostPort.getPort(), getProxyProtocol(route));
        } 
        
    }
    
    @Autowired
    ProxyChecker proxyChecker;
    
    @PostConstruct
    public void postConstruct() {
        updateRoutes();
        ProxyRegistry.setChecker(proxyChecker);
    }
    
    @Override
    public Endpoint getOrCreateEndpoint(RedisMeta redisMeta) {
        HostPort hostPort = new HostPort(redisMeta.getIp(), redisMeta.getPort());
        return getOrCreateEndpoint(hostPort);
    }

    @Override
    public void remove(HostPort hostPort) {
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

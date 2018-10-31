package com.ctrip.xpipe.redis.console.healthcheck.impl;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.proxy.ProxyEnabledEndpoint;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentMap;

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

    @Override
    public Endpoint getOrCreateEndpoint(RedisMeta redisMeta) {
        HostPort hostPort = new HostPort(redisMeta.getIp(), redisMeta.getPort());
        return getOrCreateEndpoint(hostPort);
    }

    @Override
    public Endpoint getOrCreateEndpoint(HostPort hostPort) {
        Endpoint endpoint = map.get(hostPort);
        if(endpoint != null) {
            return endpoint;
        }
        RouteMeta route = metaCache.getRouteIfPossible(hostPort);
        if(route == null) {
            endpoint = new DefaultEndPoint(hostPort.getHost(), hostPort.getPort());
        } else {
            endpoint = new ProxyEnabledEndpoint(hostPort.getHost(), hostPort.getPort(), getProxyProtocol(hostPort, route));
        }
        map.put(hostPort, endpoint);
        return endpoint;
    }

    private ProxyConnectProtocol getProxyProtocol(HostPort hostPort, RouteMeta route) {
        String uri = String.format("%s://%s:%d", ProxyEndpoint.PROXY_SCHEME.TCP, hostPort.getHost(), hostPort.getPort());
        String protocol = String.format("%s %s %s %s;", ProxyConnectProtocol.KEY_WORD, PROXY_OPTION.ROUTE, route.getRouteInfo(), uri);
        return new DefaultProxyConnectProtocolParser().read(protocol);
    }

    @VisibleForTesting
    public DefaultHealthCheckEndpointFactory setMetaCache(MetaCache metaCache) {
        this.metaCache = metaCache;
        return this;
    }
}

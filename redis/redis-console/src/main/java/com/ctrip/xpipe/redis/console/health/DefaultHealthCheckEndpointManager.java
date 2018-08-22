package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.proxy.ProxyProtocol;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.proxy.DefaultProxyProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.utils.MapUtils;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentMap;

/**
 * @author chen.zhu
 * <p>
 * Aug 21, 2018
 */
@Component
public class DefaultHealthCheckEndpointManager implements HealthCheckEndpointManager {

    private static final Logger logger = LoggerFactory.getLogger(DefaultHealthCheckEndpointManager.class);

    @Autowired
    private MetaCache metaCache;

    private ConcurrentMap<HostPort, HealthCheckEndpoint> endpoints = Maps.newConcurrentMap();

    @Override
    public HealthCheckEndpoint getOrCreate(RedisMeta redisMeta) {
        HostPort hostPort = new HostPort(redisMeta.getIp(), redisMeta.getPort());
        return MapUtils.getOrCreate(endpoints, hostPort, new ObjectFactory<HealthCheckEndpoint>() {
            @Override
            public HealthCheckEndpoint create() {
                logger.info("[create][begin] redis: {}", redisMeta);
                HealthCheckEndpoint endpoint = newHealthCheckEndpoint(redisMeta);
                logger.info("[create][end] redis: {}, endpoint: {}", redisMeta, endpoint);
                return endpoint;
            }
        });
    }

    @Override
    public HealthCheckEndpoint getOrCreate(HostPort hostPort) {
        HealthCheckEndpoint endpoint = endpoints.get(hostPort);
        if(endpoint == null) {
            endpoint = newHealthCheckEndpoint(new RedisMeta().setIp(hostPort.getHost()).setPort(hostPort.getPort()));
        }
        return endpoint;
    }

    private HealthCheckEndpoint newHealthCheckEndpoint(RedisMeta redisMeta) {
        HealthCheckEndpoint endpoint = null;
        RouteMeta route = metaCache.getRouteIfPossible(new HostPort(redisMeta.getIp(), redisMeta.getPort()));
        if(route == null) {
            endpoint = new DefaultHealthCheckEndpoint(redisMeta);
        } else {
            endpoint = new DefaultProxyEnabledHealthCheckEndpoint(redisMeta, getProxyProtocol(redisMeta, route));
        }
        return endpoint;
    }

    private ProxyProtocol getProxyProtocol(RedisMeta redisMeta, RouteMeta route) {
        String uri = String.format("%s://%s:%d", ProxyEndpoint.PROXY_SCHEME.TCP, redisMeta.getIp(), redisMeta.getPort());
        String protocol = String.format("%s %s %s %s;", ProxyProtocol.KEY_WORD, PROXY_OPTION.ROUTE, route.getRouteInfo(), uri);
        return new DefaultProxyProtocolParser().read(protocol);
    }
}

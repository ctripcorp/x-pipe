package com.ctrip.xpipe.redis.core.protocal.cmd.proxy.impl;

import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.RedisProxy;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class XpipeRedisProxy implements RedisProxy {
//    ProxyConnectProtocol protocol;
    ProxyConnectProtocol proxy_servers;
    ProxyConnectProtocol proxy_params;
    static Logger logger = LoggerFactory.getLogger(XpipeRedisProxy.class);

    boolean ProxyConnectProtocolEqual(ProxyConnectProtocol source, ProxyConnectProtocol target) {
        if(source == null && target == null) return true;
        if(source != null && target != null && source.getRouteInfo().equals(target.getRouteInfo())) {
            return true;
        }
        return false;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        XpipeRedisProxy that = (XpipeRedisProxy) o;
        if(!ProxyConnectProtocolEqual(proxy_servers, that.proxy_servers)) {
            return false;
        }

        if (!ProxyConnectProtocolEqual(proxy_params, that.proxy_params)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(proxy_servers, proxy_params);
    }

    @Override
    public String[] getRequest() {
        String[] params = ArrayUtils.addAll(null, "proxy-type", "XPIPE-PROXY", "proxy-server", String.join(",", proxy_servers.nextEndpoints().stream().map(
                endpoint -> {
                    return String.format("%s:%d", endpoint.getHost(), endpoint.getPort());
                }
        ).collect(Collectors.toList())));
        if(proxy_params != null && proxy_params.getRouteInfo() != null) {
            params = ArrayUtils.addAll(params,  "proxy-params", proxy_params.getRouteInfo());
        }
        return params;
    }

    public String getParams() {
        if(proxy_params == null) return null;
        return proxy_params.getRouteInfo();
    }

    public static XpipeRedisProxy read(String info) {
        XpipeRedisProxy xpipe = new XpipeRedisProxy();
        try {
            logger.info("[XpipeRedisProxy] read:" + info);
            List<String> servers = new LinkedList<>();
            List<String> params = new LinkedList<>();

            Arrays.stream(info.split("\\s")).forEach(option -> {
                if(option.startsWith(ProxyEndpoint.PROXY_SCHEME.PROXYTCP.name())) {
                    servers.add(option);
                } else if(option.startsWith(ProxyEndpoint.PROXY_SCHEME.PROXYTLS.name())) {
                    params.add(option);
                }
            });
            DefaultProxyConnectProtocolParser parser = new DefaultProxyConnectProtocolParser();
            if(servers.size() > 0) {
                xpipe.proxy_servers = parser.read("PROXY ROUTE " + String.join(" ", servers));
            }
            parser = new DefaultProxyConnectProtocolParser();
            if(params.size() > 0) {
                xpipe.proxy_params = parser.read("PROXY ROUTE " + String.join(" ", params));
            }


        } catch (Exception e) {
            throw  e;
        }
        return xpipe;
    }

    public List<ProxyEndpoint> getServers() {
        return this.proxy_servers.nextEndpoints();
    }
}
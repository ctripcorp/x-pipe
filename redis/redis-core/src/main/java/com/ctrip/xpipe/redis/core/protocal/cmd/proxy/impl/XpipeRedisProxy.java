package com.ctrip.xpipe.redis.core.protocal.cmd.proxy.impl;

import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.RedisProxy;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.exception.XPipeProxyResultException;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.protocols.DefaultProxyConnectProtocol;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class XpipeRedisProxy implements RedisProxy {
    List<ProxyEndpoint> servers = new LinkedList<>();
    List<ProxyEndpoint> proxytls = new LinkedList<>();
    DefaultProxyConnectProtocol protocol;
    static Logger logger = LoggerFactory.getLogger(XpipeRedisProxy.class);

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        XpipeRedisProxy that = (XpipeRedisProxy) o;
        return Objects.equals(servers, that.servers) &&
                Objects.equals(proxytls, that.proxytls) &&
                Objects.equals(protocol, that.protocol);
    }

    @Override
    public int hashCode() {
        return Objects.hash(servers, proxytls, protocol);
    }

    @Override
    public String[] getParams() {
        String[] params = ArrayUtils.addAll(null, "proxy-type", "XPIPE-PROXY", "proxy-server", String.join(",", servers.stream().map(
                endpoint -> {
                    return String.format("%s:%d", endpoint.getHost(), endpoint.getPort());
                }
        ).collect(Collectors.toList())));
        if(proxytls != null && proxytls.size() > 0) {
            params = ArrayUtils.addAll(params,  "proxy-proxytls", String.join(",", proxytls.stream().map(
                    endpoint -> {
                        return String.format("PROXYTLS://%s:%d", endpoint.getHost(), endpoint.getPort());
                    }
            ).collect(Collectors.toList())));
        }
        return params;
    }
    public void addServer(ProxyEndpoint point) {
        this.servers.add(point);
    }
    public void addTLS(ProxyEndpoint point) {
        this.proxytls.add(point);
    }

    public static XpipeRedisProxy read(String info) {
        XpipeRedisProxy xpipe = new XpipeRedisProxy();
        try {
            System.out.println("[XpipeRedisProxy] read:" + info);
            String[] splits = info.trim().split(" ");
            for(int i = 0; i < splits.length; i++) {
                String[] list = splits[i].trim().split(",");
                for(int j = 0; j < list.length; j++) {
                    DefaultProxyEndpoint proxyEndpoint = new DefaultProxyEndpoint(list[j]);
                    if(proxyEndpoint.getScheme().equals("PROXYTCP")) {
                        xpipe.addServer(proxyEndpoint);
                    } else if(proxyEndpoint.getScheme().equals("PROXYTLS")) {
                        xpipe.addTLS(proxyEndpoint);
                    } else {
                        logger.error("[XpipeRedisProxy]read RouteMeta Error: {}" + info);
                        throw new XPipeProxyResultException(String.format("parse ProxyEndpoint Error: %s", info));
                    }
                }
            }
        } catch (Exception e) {
            throw  e;
        }
        return xpipe;
    }

    public List<ProxyEndpoint> getServers() {
        return servers;
    }
}
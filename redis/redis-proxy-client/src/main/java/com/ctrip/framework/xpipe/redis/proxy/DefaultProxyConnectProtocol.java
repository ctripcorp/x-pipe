package com.ctrip.framework.xpipe.redis.proxy;

import com.ctrip.framework.xpipe.redis.ProxyRegistry;

import java.net.InetSocketAddress;
import java.util.List;

import static com.ctrip.framework.xpipe.redis.proxy.RouteOptionParser.WHITE_SPACE;

public class DefaultProxyConnectProtocol implements ProxyConnectProtocol {

    private RouteOptionParser routeOptionParser = new RouteOptionParser();

    public DefaultProxyConnectProtocol(String protocol) {
        routeOptionParser.read(removeKeyWord(protocol));
    }

    @Override
    public List<InetSocketAddress> nextEndpoints() {
        return routeOptionParser.getNextEndpoints();
    }

    @Override
    public byte[] output() {
        StringBuilder proxyProtocol = new StringBuilder("+").append(ProxyRegistry.PROXY_KEY_WORD).append(WHITE_SPACE);
        proxyProtocol.append(routeOptionParser.output()).append(";\r\n");
        return proxyProtocol.toString().getBytes();
    }

    protected String removeKeyWord(String protocol) {
        return protocol.substring(ProxyRegistry.PROXY_KEY_WORD.length()).trim();
    }
}

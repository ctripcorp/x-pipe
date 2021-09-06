package com.ctrip.framework.xpipe.redis.proxy;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

public class DefaultProxyResourceManager implements ProxyResourceManager {

    private Random random = new Random();

    private final ProxyConnectProtocol proxyConnectProtocol;

    private final List<ProxyInetSocketAddress> candidates;

    public DefaultProxyResourceManager(ProxyConnectProtocol proxyConnectProtocol, List<ProxyInetSocketAddress> candidates) {
        this.proxyConnectProtocol = proxyConnectProtocol;
        this.candidates = candidates;
    }

    @Override
    public byte[] getProxyConnectProtocol() {
        return proxyConnectProtocol.output();
    }

    @Override
    public InetSocketAddress nextHop() {
        if (candidates == null || candidates.isEmpty()) {
            return null;
        }
        int index = random.nextInt(candidates.size());
        return candidates.get(index);
    }
}

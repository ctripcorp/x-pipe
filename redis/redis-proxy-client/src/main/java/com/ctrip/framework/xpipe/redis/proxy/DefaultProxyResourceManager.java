package com.ctrip.framework.xpipe.redis.proxy;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

public class DefaultProxyResourceManager implements ProxyResourceManager {

    private Random random = new Random();

    private ProxyConnectProtocol proxyConnectProtocol;

    private List<InetSocketAddress> candidates;

    public DefaultProxyResourceManager(ProxyConnectProtocol proxyConnectProtocol) {
        this.proxyConnectProtocol = proxyConnectProtocol;
        this.candidates = proxyConnectProtocol.nextEndpoints();
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

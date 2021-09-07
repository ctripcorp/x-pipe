package com.ctrip.framework.xpipe.redis.proxy;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

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
        List<ProxyInetSocketAddress> addresses = new ArrayList<>();
        for (ProxyInetSocketAddress node : candidates) {
            if (!node.down) {
                addresses.add(node);
            }
        }
        int index = random.nextInt(addresses.size());
        return addresses.get(index);
    }
    
    public List<ProxyInetSocketAddress> nextEndpoints() {
        return candidates;
    }
}

package com.ctrip.framework.xpipe.redis.utils;

import com.ctrip.framework.xpipe.redis.proxy.DefaultProxyConnectProtocol;
import com.ctrip.framework.xpipe.redis.proxy.DefaultProxyResourceManager;
import com.ctrip.framework.xpipe.redis.proxy.ProxyConnectProtocol;
import com.ctrip.framework.xpipe.redis.proxy.ProxyResourceManager;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ProxyUtil extends ConcurrentHashMap<SocketAddress, ProxyResourceManager> {

    private ConcurrentMap<Object, SocketAddress> socketAddressMap = new ConcurrentHashMap<>();

    private static class ProxyResourceHolder {
        public static final ProxyUtil INSTANCE = new ProxyUtil();
    }

    public static ProxyUtil getInstance() {
        return ProxyResourceHolder.INSTANCE;
    }

    public synchronized void registerProxy(String ip, int port, String routeInfo) {
        put(new InetSocketAddress(ip, port), getProxyProtocol(ip, port, routeInfo));
    }

    public synchronized ProxyResourceManager unregisterProxy(String ip, int port) {
        ProxyResourceManager proxyResourceManager = remove(new InetSocketAddress(ip, port));
        return proxyResourceManager;
    }

    protected boolean needProxy(SocketAddress socketAddress){
        return containsKey(socketAddress);
    }

    protected InetSocketAddress getProxyAddress(Object o, SocketAddress socketAddress) {
        socketAddressMap.put(o, socketAddress);
        ProxyResourceManager proxyResourceManager = get(socketAddress);
        InetSocketAddress proxyEndpoint = proxyResourceManager.nextHop();
        return proxyEndpoint;
    }

    protected byte[] getProxyConnectProtocol(Object object){
        SocketAddress socketAddress = socketAddressMap.get(object);
        ProxyResourceManager proxyResourceManager = get(socketAddress);
        return proxyResourceManager.getProxyConnectProtocol();
    }

    protected SocketAddress removeProxyAddress(Object o) {
        return socketAddressMap.remove(o);
    }

    private ProxyResourceManager getProxyProtocol(String ip, int port, String routeInfo) {
        String protocol = String.format("%s://%s:%s", routeInfo.trim(), ip, port);
        ProxyConnectProtocol proxyConnectProtocol = new DefaultProxyConnectProtocol(protocol);
        ProxyResourceManager proxyResourceManager = new DefaultProxyResourceManager(proxyConnectProtocol);
        return proxyResourceManager;
    }

}

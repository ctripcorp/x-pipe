package com.ctrip.framework.xpipe.redis.utils;

import com.ctrip.framework.xpipe.redis.proxy.DefaultProxyResourceManager;
import com.ctrip.framework.xpipe.redis.proxy.ProxyResourceManager;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import io.netty.buffer.ByteBuf;

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

    public synchronized ProxyResourceManager unregisterProxy(String ip, int port) throws Exception {
        ProxyResourceManager proxyResourceManager = remove(new InetSocketAddress(ip, port));
        if (proxyResourceManager != null) {
            proxyResourceManager.stop();
        }
        return proxyResourceManager;
    }

    protected boolean needProxy(SocketAddress socketAddress){
        return containsKey(socketAddress);
    }

    protected InetSocketAddress getProxyAddress(Object o, SocketAddress socketAddress) {
        socketAddressMap.put(o, socketAddress);
        ProxyResourceManager proxyResourceManager = get(socketAddress);
        ProxyEndpoint proxyEndpoint = proxyResourceManager.nextHop();
        return proxyEndpoint.getSocketAddress();
    }

    protected byte[] getProxyConnectProtocol(Object object){
        SocketAddress socketAddress = socketAddressMap.get(object);
        ProxyResourceManager proxyResourceManager = get(socketAddress);
        ByteBuf protocol = proxyResourceManager.getProxyConnectProtocol();
        byte[] bytes = new byte[protocol.readableBytes()];
        protocol.readBytes(bytes);
        return bytes;
    }

    protected SocketAddress removeProxyAddress(Object o) {
        return socketAddressMap.remove(o);
    }

    private ProxyResourceManager getProxyProtocol(String ip, int port, String routeInfo) {
        String protocol = String.format("%s://%s:%s", routeInfo.trim(), ip, port);
        ProxyConnectProtocol proxyConnectProtocol =  new DefaultProxyConnectProtocolParser().read(protocol);
        ProxyResourceManager proxyResourceManager = new DefaultProxyResourceManager(proxyConnectProtocol);
        return proxyResourceManager;
    }

}

package com.ctrip.framework.xpipe.redis.utils;

import com.ctrip.framework.xpipe.redis.ProxyChecker;
import com.ctrip.framework.xpipe.redis.proxy.*;
import com.google.common.annotations.VisibleForTesting;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.BiConsumer;

public class ProxyUtil extends ConcurrentHashMap<SocketAddress, ProxyResourceManager> implements ThreadFactory {
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
        InetSocketAddress address = new InetSocketAddress(ip, port);
        ProxyResourceManager proxyResourceManager = remove(address);
        if(proxyResourceManager != null) {
            proxyResourceManager.nextEndpoints().stream().forEach(endpoint -> {
                if (--proxies.get(endpoint).reference == 0) {
                    proxies.remove(endpoint);
                }
            });
        }
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
        List<InetSocketAddress> endpoints = proxyConnectProtocol.nextEndpoints();
        List<ProxyInetSocketAddress> next = new ArrayList<>();
        for (InetSocketAddress endpoint : endpoints) {
            next.add(getOrCreateProxy(endpoint));
        }
        ProxyResourceManager proxyResourceManager = new DefaultProxyResourceManager(proxyConnectProtocol, next);
        return proxyResourceManager;
    }

    private ScheduledExecutorService scheduled = Executors.newSingleThreadScheduledExecutor(this);

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setName("proxy-checker-background-schedule");
        return thread;
    }

    private void check(ProxyInetSocketAddress proxy) {
        
        checker.check(proxy).whenComplete(new BiConsumer<Boolean, Throwable>() {
            @Override
            public void accept(Boolean aBoolean, Throwable throwable) {
                if(throwable != null  || aBoolean == false ) {
                    proxy.down = true;
                } else {
                    proxy.sick = false;
                    proxy.down = false;
                }
            }
        });
    }

    private volatile  int checkInterval = 1000; //seconds
    ScheduledFuture future = null;
    public ProxyUtil() {
        startCheck(); 
    }
    
    void startCheck() {
        if(future != null) return;
        future = scheduled.scheduleWithFixedDelay(()->{
            try {
                if (checker != null) {
                    for (ProxyInetSocketAddress proxy : proxies.values()) {
                        if (proxy.sick) check(proxy);
                    }
                }
            } catch (Throwable e) {
                e.printStackTrace();
            }
            
        }, checkInterval, checkInterval, TimeUnit.MILLISECONDS);
    }
    
    void stopCheck() {
        if(future == null) return;
        future.cancel(true);
        future = null;
    }

    public void setCheckInterval(int checkInterval) {
        this.checkInterval = checkInterval;
        stopCheck();
        startCheck();
    }

    private ConcurrentMap<InetSocketAddress, ProxyInetSocketAddress> proxies = new ConcurrentHashMap<>();

    private volatile ProxyChecker checker = null;

    public void setChecker(ProxyChecker checker) {
        this.checker = checker;
    }

    @VisibleForTesting
    ProxyInetSocketAddress getOrCreateProxy(InetSocketAddress endpoint) {
        ProxyInetSocketAddress proxy = proxies.computeIfAbsent(endpoint, e->new ProxyInetSocketAddress(e.getAddress(), e.getPort()));
        proxy.reference += 1;
        return proxy;
    }
}

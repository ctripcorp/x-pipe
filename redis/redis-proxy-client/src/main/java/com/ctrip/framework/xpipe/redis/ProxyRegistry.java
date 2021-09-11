package com.ctrip.framework.xpipe.redis;

import com.ctrip.framework.xpipe.redis.proxy.ProxyInetSocketAddress;
import com.ctrip.framework.xpipe.redis.proxy.ProxyResourceManager;
import com.ctrip.framework.xpipe.redis.utils.ProxyUtil;

import java.net.InetSocketAddress;
import java.util.function.Consumer;

import static com.ctrip.framework.xpipe.redis.utils.Constants.PROXY_KEY_WORD;

public class ProxyRegistry {

    public static boolean registerProxy(String ip, int port, String routeInfo) {
        if (routeInfo != null && routeInfo.startsWith(PROXY_KEY_WORD)) {
            ProxyUtil.getInstance().registerProxy(ip, port, routeInfo);
            return true;
        }
        return false;
    }

    public static ProxyResourceManager unregisterProxy(String ip, int port) {
        return ProxyUtil.getInstance().unregisterProxy(ip, port);
    }
    
    public static ProxyResourceManager getProxy(String ip, int port) {
        return ProxyUtil.getInstance().get(new InetSocketAddress(ip, port));
    }
    
    public static void setChecker(ProxyChecker checker) {
        ProxyUtil.getInstance().setChecker(checker);
    }
    
    public static void setCheckInterval(int interval) {
        ProxyUtil.getInstance().setCheckInterval(interval);
    }

    public static void onProxyUp(Consumer<ProxyInetSocketAddress> upAction) {
        ProxyUtil.getInstance().onProxyUp(upAction);
    }

    public static void onProxyDown(Consumer<ProxyInetSocketAddress> downAction) {
        ProxyUtil.getInstance().onProxyDown(downAction);
    }
}

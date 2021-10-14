package com.ctrip.framework.xpipe.redis;

import com.ctrip.framework.xpipe.redis.proxy.ProxyInetSocketAddress;
import com.ctrip.framework.xpipe.redis.proxy.ProxyResourceManager;
import com.ctrip.framework.xpipe.redis.utils.ProxyUtil;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

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
    
    public static void setChecker(
            Function<InetSocketAddress, CompletableFuture<Boolean>> check
            , Supplier<Integer> getRetryUpTimes
            , Supplier<Integer> getRetryDownTimes) {
        ProxyUtil.getInstance().setChecker(new ProxyChecker() {
            @Override
            public CompletableFuture<Boolean> check(InetSocketAddress address) {
                return check.apply(address);
            }

            @Override
            public int getRetryUpTimes() {
                return getRetryUpTimes.get();
            }

            @Override
            public int getRetryDownTimes() {
                return getRetryDownTimes.get();
            }
        });
    }
    
    public static void startCheck() {
        ProxyUtil.getInstance().startCheck();
    }
    
    public static void stopCheck() {
        ProxyUtil.getInstance().stopCheck();
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

package com.ctrip.framework.xpipe.redis;

import com.ctrip.framework.xpipe.redis.proxy.ProxyResourceManager;
import com.ctrip.framework.xpipe.redis.utils.ProxyUtil;

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

}

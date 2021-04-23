package com.ctrip.framework.xpipe.redis;

import com.ctrip.framework.xpipe.redis.proxy.ProxyResourceManager;
import com.ctrip.framework.xpipe.redis.utils.ProxyUtil;
import org.apache.commons.lang3.StringUtils;

import static com.ctrip.xpipe.api.proxy.ProxyProtocol.KEY_WORD;


public class ProxyRegistry {

    public static boolean registerProxy(String ip, int port, String routeInfo) {
        if (StringUtils.isNotBlank(routeInfo) && routeInfo.startsWith(KEY_WORD)) {
            ProxyUtil.getInstance().registerProxy(ip, port, routeInfo);
            return true;
        }
        return false;
    }

    public static ProxyResourceManager unregisterProxy(String ip, int port) throws Exception {
        return ProxyUtil.getInstance().unregisterProxy(ip, port);
    }

}

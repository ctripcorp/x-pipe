package com.ctrip.xpipe.redis.proxy.model;

import com.ctrip.xpipe.redis.core.proxy.ProxyProtocol;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public class TunnelMeta {

    private SessionMeta frontEnd;

    private SessionMeta backEnd;

    public static TunnelMeta fromProxyProtocol(ProxyProtocol protocol) {
        TunnelMeta meta = new TunnelMeta();
        return meta;
    }
}

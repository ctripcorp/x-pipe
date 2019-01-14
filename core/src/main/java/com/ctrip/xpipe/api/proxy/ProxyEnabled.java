package com.ctrip.xpipe.api.proxy;

/**
 * @author chen.zhu
 * <p>
 * May 13, 2018
 */
public interface ProxyEnabled {

    ProxyConnectProtocol getProxyProtocol();

    default boolean isSameWith(ProxyEnabled other) {
        return this.getProxyProtocol().getRouteInfo().equalsIgnoreCase(other.getProxyProtocol().getRouteInfo());
    }
}

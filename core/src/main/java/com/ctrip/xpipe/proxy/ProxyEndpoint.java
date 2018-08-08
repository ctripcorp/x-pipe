package com.ctrip.xpipe.proxy;

import com.ctrip.xpipe.api.endpoint.Endpoint;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public interface ProxyEndpoint extends Endpoint {

    boolean isSslEnabled();

    boolean isProxyProtocolSupported();

    String getUri();

    enum PROXY_SCHEME {
        TCP, TLS, PROXYTCP, PROXYTLS;

        public boolean matches(String scheme) {
            return name().equalsIgnoreCase(scheme);
        }
    }
}

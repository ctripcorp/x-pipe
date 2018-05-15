package com.ctrip.xpipe.redis.core.proxy.endpoint;

import com.ctrip.xpipe.api.endpoint.Endpoint;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public interface ProxyEndpoint extends Endpoint {

    boolean isSslEnabled();

    String rawUri();

    enum PROXY_SCHEME {
        PROXY, PROXYTLS;

        boolean matches(String scheme) {
            return name().equalsIgnoreCase(scheme);
        }
    }
}

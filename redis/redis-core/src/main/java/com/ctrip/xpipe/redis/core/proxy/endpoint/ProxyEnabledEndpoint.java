package com.ctrip.xpipe.redis.core.proxy.endpoint;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.proxy.ProxyEnabled;
import com.ctrip.xpipe.redis.core.proxy.ProxyProtocol;

import java.net.InetSocketAddress;

/**
 * @author chen.zhu
 * <p>
 * May 31, 2018
 */
public class ProxyEnabledEndpoint extends DefaultEndPoint implements Endpoint, ProxyEnabled {

    private ProxyProtocol protocol;

    public ProxyEnabledEndpoint(String ip, int port, ProxyProtocol protocol) {
        super(ip, port);
        this.protocol = protocol;
    }

    public ProxyEnabledEndpoint(InetSocketAddress address, ProxyProtocol protocol) {
        super(address);
        this.protocol = protocol;
    }

    @Override
    public ProxyProtocol getProxyProtocol() {
        return protocol;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}

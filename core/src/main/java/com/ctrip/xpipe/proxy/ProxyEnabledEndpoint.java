package com.ctrip.xpipe.proxy;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyEnabled;
import com.ctrip.xpipe.api.proxy.ProxyProtocol;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;

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

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}

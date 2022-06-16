package com.ctrip.xpipe.proxy;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyEnabled;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.net.InetSocketAddress;

/**
 * @author chen.zhu
 * <p>
 * May 31, 2018
 */
public class ProxyEnabledEndpoint extends DefaultEndPoint implements Endpoint, ProxyEnabled {

    @JsonIgnore
    private ProxyConnectProtocol protocol;

    public ProxyEnabledEndpoint(String ip, int port, ProxyConnectProtocol protocol) {
        super(ip, port);
        this.protocol = protocol;
    }

    public ProxyEnabledEndpoint(InetSocketAddress address, ProxyConnectProtocol protocol) {
        super(address);
        this.protocol = protocol;
    }

    @Override
    @JsonIgnore
    public ProxyConnectProtocol getProxyProtocol() {
        return protocol;
    }

    @Override
    public String toString() {
        return String.format("endpoint: %s, proxy info: %s", super.toString(), getProxyProtocol());
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

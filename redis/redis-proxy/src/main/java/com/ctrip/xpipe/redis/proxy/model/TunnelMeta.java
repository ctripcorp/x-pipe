package com.ctrip.xpipe.redis.proxy.model;

import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnel;

import java.io.Serializable;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public class TunnelMeta implements Serializable {

    private TunnelIdentity identity;

    private String state;

    private String protocol;

    private SessionMeta frontend;

    private SessionMeta backend;

    public TunnelMeta(DefaultTunnel tunnel, ProxyConnectProtocol protocol) {
        this.identity = tunnel.identity();
        this.state = tunnel.getState().name();
        this.protocol = protocol.getContent();
        this.frontend = tunnel.frontend().getSessionMeta();
        this.backend = tunnel.backend().getSessionMeta();
    }

    public TunnelMeta(TunnelIdentity identity, String state, String protocol, SessionMeta frontend, SessionMeta backend) {
        this.identity = identity;
        this.state = state;
        this.protocol = protocol;
        this.frontend = frontend;
        this.backend = backend;
    }

    public TunnelIdentity getIdentity() {
        return identity;
    }

    public TunnelMeta setIdentity(TunnelIdentity identity) {
        this.identity = identity;
        return this;
    }

    public String getState() {
        return state;
    }

    public TunnelMeta setState(String state) {
        this.state = state;
        return this;
    }

    public String getProtocol() {
        return protocol;
    }

    public TunnelMeta setProtocol(String protocol) {
        this.protocol = protocol;
        return this;
    }

    public SessionMeta getFrontend() {
        return frontend;
    }

    public TunnelMeta setFrontend(SessionMeta frontend) {
        this.frontend = frontend;
        return this;
    }

    public SessionMeta getBackend() {
        return backend;
    }

    public TunnelMeta setBackend(SessionMeta backend) {
        this.backend = backend;
        return this;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer("{");
        sb.append("identity: ").append(identity.toString()).append(",");
        sb.append("state: ").append(state).append(",");
        sb.append("protocol: ").append(protocol).append(",");
        sb.append("frontend: ").append(frontend.toString()).append(",");
        sb.append("backend: ").append(backend.toString());
        return sb.toString();
    }
}

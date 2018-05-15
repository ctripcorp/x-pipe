package com.ctrip.xpipe.redis.proxy.model;

import com.ctrip.xpipe.redis.core.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnel;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public class TunnelMeta {

    private String identity;

    private String state;

    private String protocol;

    private SessionMeta frontend;

    private SessionMeta backend;

    public TunnelMeta(DefaultTunnel tunnel, ProxyProtocol protocol) {
        this.identity = tunnel.identity();
        this.state = tunnel.getState().name();
        this.protocol = protocol.getContent();
        this.frontend = tunnel.frontend().getSessionMeta();
        this.backend = tunnel.backend().getSessionMeta();
    }

    public TunnelMeta(String identity, String state, String protocol, SessionMeta frontend, SessionMeta backend) {
        this.identity = identity;
        this.state = state;
        this.protocol = protocol;
        this.frontend = frontend;
        this.backend = backend;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer("{");
        sb.append("identity: ").append(identity).append(",");
        sb.append("state: ").append(state).append(",");
        sb.append("protocol: ").append(protocol).append(",");
        sb.append("frontend: ").append(frontend.toString()).append(",");
        sb.append("backend: ").append(backend.toString());
        return sb.toString();
    }
}

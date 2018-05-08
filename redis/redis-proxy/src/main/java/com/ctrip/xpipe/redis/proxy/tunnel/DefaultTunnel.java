package com.ctrip.xpipe.redis.proxy.tunnel;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointManager;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.model.TunnelMeta;
import com.ctrip.xpipe.redis.proxy.session.DefaultSessionManager;
import com.ctrip.xpipe.redis.proxy.session.SessionManager;
import io.netty.channel.Channel;

/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */
public class DefaultTunnel extends AbstractLifecycle implements Tunnel {

    private Channel frontendChannel;

    private ProxyProtocol protocol;

    private SessionManager sessionManager;

    private ProxyEndpointManager endpointManager;

    private TunnelState tunnelState;

    public DefaultTunnel(Channel frontendChannel, ProxyEndpointManager endpointManager, ProxyProtocol protocol) {
        this.endpointManager = endpointManager;
        this.frontendChannel = frontendChannel;
        this.protocol = protocol;
        sessionManager = new DefaultSessionManager(this);
    }

    @Override
    public Channel frontendChannel() {
        return frontendChannel;
    }

    @Override
    public Session frontEnd() {
        return sessionManager.frontend();
    }

    @Override
    public Session backEnd() {
        return sessionManager.backend();
    }

    @Override
    public void disconnect() {
        tunnelState.disconnect();
    }

    @Override
    public TunnelMeta getTunnelMeta() {
        return null;
    }

    @Override
    public void setProxyProtocol(ProxyProtocol protocol) {
        this.protocol = protocol;
    }

    @Override
    public ProxyProtocol getProxyProtocol() {
        return protocol;
    }

    @Override
    public void setProxyEndpointManager(ProxyEndpointManager manager) {

    }

    @Override
    public ProxyEndpoint getNextJump() {
        return null;
    }

    @Override
    public void setTunnelState() {

    }
}

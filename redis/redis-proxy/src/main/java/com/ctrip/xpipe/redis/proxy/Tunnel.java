package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.redis.core.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointManager;
import com.ctrip.xpipe.redis.proxy.model.TunnelMeta;
import io.netty.channel.Channel;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public interface Tunnel extends Lifecycle {

    Channel frontendChannel();

    Session frontEnd();

    Session backEnd();

    void disconnect();

    TunnelMeta getTunnelMeta();

    void setProxyProtocol(ProxyProtocol protocol);

    ProxyProtocol getProxyProtocol();

    void setProxyEndpointManager(ProxyEndpointManager manager);

    ProxyEndpoint getNextJump();

    void setTunnelState();

    enum TUNNEL_STATE {
        FRONT_END_ESTABLISHED,
        PROXY_PROTOCOL_RECEIVED,
        BACK_END_SELECTED,
        BACK_END_ESTABLISHED,
        PROXY_PROTOCOL_SENT,
        FRONT_END_TERMINATED,
        BACK_END_TERMINATED
    }
}

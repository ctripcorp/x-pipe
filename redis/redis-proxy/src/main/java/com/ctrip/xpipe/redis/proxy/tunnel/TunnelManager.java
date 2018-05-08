package com.ctrip.xpipe.redis.proxy.tunnel;

import com.ctrip.xpipe.redis.core.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpoint;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import io.netty.channel.Channel;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public interface TunnelManager {

    Tunnel getOrCreate(Channel frontendChannel, ProxyProtocol protocol);

    void remove(Channel frontendChannel);

    List<Tunnel> tunnels();
}

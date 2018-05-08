package com.ctrip.xpipe.redis.proxy.tunnel;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.redis.core.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointManager;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.utils.MapUtils;
import com.google.common.collect.Maps;
import io.netty.channel.Channel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */
@Component
public class DefaultTunnelManager implements TunnelManager {

    @Autowired
    private ProxyEndpointManager endpointManager;

    private Map<Channel, Tunnel> cache = Maps.newConcurrentMap();

    @Override
    public Tunnel getOrCreate(Channel frontendChannel, ProxyProtocol protocol) {
        return MapUtils.getOrCreate(cache, frontendChannel, new ObjectFactory<Tunnel>() {
            @Override
            public Tunnel create() {
                DefaultTunnel tunnel = new DefaultTunnel(frontendChannel, endpointManager, protocol);
                return tunnel;
            }
        });
    }

    @Override
    public void remove(Channel frontendChannel) {

    }

    @Override
    public List<Tunnel> tunnels() {
        return null;
    }
}

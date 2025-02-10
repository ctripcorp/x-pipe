package com.ctrip.xpipe.redis.proxy.tunnel;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import io.netty.channel.Channel;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public interface TunnelManager extends Releasable, Observer {

    Tunnel create(Channel frontendChannel, ProxyConnectProtocol protocol);

    void remove(Channel frontendChannel);

    void removeAll();

    List<Tunnel> tunnels();

    Tunnel getById(String id);

    void markAllUnRead();

}

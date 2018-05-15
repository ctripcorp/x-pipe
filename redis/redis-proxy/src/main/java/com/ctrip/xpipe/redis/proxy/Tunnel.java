package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpoint;
import com.ctrip.xpipe.redis.proxy.model.TunnelMeta;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelState;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public interface Tunnel extends Lifecycle, Releasable, Observable, Observer {

    Session frontend();

    Session backend();

    Session session(Channel channel);

    TunnelMeta getTunnelMeta();

    ProxyEndpoint getNextJump();

    void setState(TunnelState tunnelState);

    TunnelState getState();

    void forward(ByteBuf message, Session src);

    String identity();

    void sendProxyProtocol();

}

package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.redis.core.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.proxy.model.TunnelMeta;
import com.ctrip.xpipe.redis.proxy.session.BackendSession;
import com.ctrip.xpipe.redis.proxy.session.FrontendSession;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelState;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public interface Tunnel extends Lifecycle, Releasable, Observable, Observer {

    FrontendSession frontend();

    BackendSession backend();

    TunnelMeta getTunnelMeta();

    void setState(TunnelState tunnelState);

    TunnelState getState();

    String identity();

    ChannelFuture forwardToBackend(ByteBuf message);

    ChannelFuture forwardToFrontend(ByteBuf message);

    void closeBackendRead();

    void closeFrontendRead();

    void triggerBackendRead();

    void triggerFrontendRead();

    ProxyProtocol getProxyProtocol();

}

package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.redis.proxy.model.TunnelIdentity;
import com.ctrip.xpipe.redis.proxy.model.TunnelMeta;
import com.ctrip.xpipe.redis.proxy.monitor.TunnelMonitor;
import com.ctrip.xpipe.redis.proxy.session.BackendSession;
import com.ctrip.xpipe.redis.proxy.session.FrontendSession;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelState;
import io.netty.buffer.ByteBuf;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public interface Tunnel extends Lifecycle, Releasable, Observable, Observer {

    FrontendSession frontend();

    BackendSession backend();

    TunnelMeta getTunnelMeta();

    TunnelState getState();

    TunnelIdentity identity();

    void forwardToBackend(ByteBuf message);

    void forwardToFrontend(ByteBuf message);

    ProxyConnectProtocol getProxyProtocol();

    TunnelMonitor getTunnelMonitor();

}

package com.ctrip.xpipe.redis.proxy.listener;

import com.ctrip.xpipe.redis.proxy.model.TunnelIdentity;
import com.ctrip.xpipe.redis.proxy.session.SESSION_TYPE;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelState;

/**
 * @author chen.zhu
 * <p>
 * Oct 15, 2018
 */
public interface TunnelStats {

    TunnelIdentity getTunnelIdentity();

    TunnelState getTunnelState();

    String getProxyProtocol();

    long getProtocolRecTime();

    long getProtocolSendTime();

    long getCloseTime();

    SESSION_TYPE closeFrom();

    CLOSE_REASON getCloseReason();

    enum CLOSE_REASON {

    }
}

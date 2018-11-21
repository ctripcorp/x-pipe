package com.ctrip.xpipe.redis.proxy.monitor.stats;

import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelStatsResult;
import com.ctrip.xpipe.redis.proxy.model.TunnelIdentity;
import com.ctrip.xpipe.redis.proxy.session.SESSION_TYPE;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelEventHandler;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelState;

/**
 * @author chen.zhu
 * <p>
 * Oct 15, 2018
 */
public interface TunnelStats extends TunnelEventHandler, Observer {

    TunnelIdentity getTunnelIdentity();

    TunnelState getTunnelState();

    long getProtocolRecTime();

    long getProtocolSendTime();

    long getCloseTime();

    SESSION_TYPE closeFrom();

    TunnelStatsResult getTunnelStatsResult();

}

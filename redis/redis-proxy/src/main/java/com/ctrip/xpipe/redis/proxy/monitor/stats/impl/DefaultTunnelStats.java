package com.ctrip.xpipe.redis.proxy.monitor.stats.impl;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelStatsResult;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.model.TunnelIdentity;
import com.ctrip.xpipe.redis.proxy.monitor.stats.TunnelStats;
import com.ctrip.xpipe.redis.proxy.session.SESSION_TYPE;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnel;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelState;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelStateChangeEvent;
import com.ctrip.xpipe.redis.proxy.tunnel.state.*;
import com.ctrip.xpipe.utils.ChannelUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chen.zhu
 * <p>
 * Oct 31, 2018
 */
public class DefaultTunnelStats implements TunnelStats {

    private static final Logger logger = LoggerFactory.getLogger(DefaultTunnelStats.class);

    private Tunnel tunnel;

    private SESSION_TYPE closeFrom;

    private long startTimestamp = System.currentTimeMillis();

    private long sendTimestamp;

    private long closeTimestamp;

    public DefaultTunnelStats(Tunnel tunnel) {
        this.tunnel = tunnel;
    }

    @Override
    public TunnelIdentity getTunnelIdentity() {
        return tunnel.identity();
    }

    @Override
    public TunnelState getTunnelState() {
        return tunnel.getState();
    }

    @Override
    public long getProtocolRecTime() {
        return startTimestamp;
    }

    @Override
    public long getProtocolSendTime() {
        return sendTimestamp;
    }

    @Override
    public long getCloseTime() {
        return closeTimestamp;
    }

    @Override
    public SESSION_TYPE closeFrom() {
        return closeFrom;
    }

    @Override
    public TunnelStatsResult getTunnelStatsResult() {
        if (tunnel.backend().getChannel() == null) {
            return null;
        }
        HostPort frontend = HostPort.fromString(ChannelUtil.getSimpleIpport(tunnel.frontend().getChannel().localAddress()));
        HostPort backend = HostPort.fromString(ChannelUtil.getSimpleIpport(tunnel.backend().getChannel().localAddress()));

        if(closeFrom != null) {
            return new TunnelStatsResult(tunnel.identity().toString(), tunnel.getState().name(), frontend, backend, getProtocolRecTime(),
                    getProtocolSendTime(), getCloseTime(), closeFrom.name());
        }
        return new TunnelStatsResult(tunnel.identity().toString(), tunnel.getState().name(), getProtocolRecTime(), getProtocolSendTime(), frontend, backend);
    }

    @Override
    public void onEstablished() {
        sendTimestamp = System.currentTimeMillis();
    }

    @Override
    public void onBackendClose() {
        closeFrom = SESSION_TYPE.BACKEND;
    }

    @Override
    public void onFrontendClose() {
        closeFrom = SESSION_TYPE.FRONTEND;
    }

    @Override
    public void onClosing() {
        closeTimestamp = System.currentTimeMillis();
        tunnel.getTunnelMonitor().record(tunnel);
    }

    @Override
    public void onClosed() {

    }

    // observer for tunnel change
    @Override
    public void update(Object args, Observable observable) {
        if(!(observable instanceof Tunnel)) {
            logger.error("[update] should observe tunnel only, not {}", observable.getClass().getName());
            return;
        }
        DefaultTunnel tunnel = (DefaultTunnel) observable;

        // deal with TunnelStateChangeEvent Only for current
        if(!(args instanceof TunnelStateChangeEvent)) {
            return;
        }
        TunnelStateChangeEvent event = (TunnelStateChangeEvent) args;
        TunnelState current = event.getCurrent();

        if(current instanceof TunnelEstablished) {
            onEstablished();

        } else if(current instanceof FrontendClosed) {
            onFrontendClose();

        } else if(current instanceof BackendClosed) {
            onBackendClose();

        } else if(current instanceof TunnelClosing) {
            onClosing();

        } else if(current instanceof TunnelClosed) {
            onClosed();
        }

    }
}

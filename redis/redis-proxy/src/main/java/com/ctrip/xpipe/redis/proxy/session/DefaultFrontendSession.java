package com.ctrip.xpipe.redis.proxy.session;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.monitor.SessionMonitor;
import com.ctrip.xpipe.redis.proxy.session.state.SessionEstablished;
import com.ctrip.xpipe.utils.ChannelUtil;
import io.netty.channel.Channel;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author chen.zhu
 * <p>
 * May 24, 2018
 */
public class DefaultFrontendSession extends AbstractSession implements FrontendSession {

    private AtomicReference<SessionState> sessionState;

    public DefaultFrontendSession(Tunnel tunnel, Channel channel, long trafficReportIntervalMillis) {
        super(tunnel, trafficReportIntervalMillis);
        this.channel = channel;
        this.sessionState = new AtomicReference<>(new SessionEstablished(this));
        try {
            if(channel.isActive() && channel.remoteAddress() instanceof InetSocketAddress) {
                this.endpoint = new DefaultProxyEndpoint((InetSocketAddress) channel.remoteAddress());
            } else {
                this.endpoint = new DefaultProxyEndpoint("0.0.0.0", -1);
            }
        } catch (Exception e) {
            this.endpoint = new DefaultProxyEndpoint("0.0.0.0", -1);
            logger.error("[construct]", e);
        }
    }

    @Override
    protected void doSetSessionState(SessionState newState) {
        SessionState oldState = this.sessionState.getAndSet(newState);
        if(oldState.equals(newState)) {
            logger.debug("[setSessionState][Frontend] already session state: {}", oldState);
        } else {
            logger.info("[setSessionState][Frontend] Session state change from {} to {} ({})", oldState, newState, getSessionMeta());
            EventMonitor.DEFAULT.logEvent(SESSION_STATE_CHANGE, String.format("[Frontend]%s -> %s",
                    oldState.toString(), newState.toString()), Collections.singletonMap("channel", ChannelUtil.getDesc(getChannel())));
            notifyObservers(new SessionStateChangeEvent(oldState, newState));
        }
    }

    @Override
    public SESSION_TYPE getSessionType() {
        return SESSION_TYPE.FRONTEND;
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        onSessionInit();
    }

    @Override
    public SessionState getSessionState() {
        return sessionState.get();
    }

    @Override
    public SessionMonitor getSessionMonitor() {
        return tunnel().getTunnelMonitor().getFrontendSessionMonitor();
    }
}

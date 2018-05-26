package com.ctrip.xpipe.redis.proxy.session;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.session.state.SessionEstablished;
import com.ctrip.xpipe.redis.proxy.tunnel.event.EventHandler;
import io.netty.channel.Channel;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
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
        this.endpoint = new DefaultProxyEndpoint((InetSocketAddress) channel.remoteAddress());
    }

    @Override
    protected void doSetSessionState(SessionState newState) {
        SessionState oldState = this.sessionState.getAndSet(newState);
        if(oldState.equals(newState)) {
            logger.debug("[setSessionState][Frontend] already session state: {}", oldState);
        } else {
            logger.info("[setSessionState][Frontend] Session state change from {} to {} ({})", oldState, newState, getSessionMeta());
            EventMonitor.DEFAULT.logEvent(SESSION_STATE_CHANGE, String.format("Session: %s, %s -> %s", getSessionMeta(),
                    oldState.toString(), newState.toString()));
            notifyObservers(new SessionStateChangeEvent(oldState, newState));
        }
    }

    @Override
    public SESSION_TYPE getSessionType() {
        return SESSION_TYPE.FRONTEND;
    }

    @Override
    protected void doInitialize() throws Exception {
        tunnel().closeFrontendRead();
        tunnel().backend().registerChannelEstablishedHandler(new EventHandler() {
            @Override
            public void handle() {
                getChannel().eventLoop()
                        .schedule(()->{
                            tunnel().triggerFrontendRead();
                        }, 1, TimeUnit.MILLISECONDS);
            }
        });
        super.doInitialize();
    }

    @Override
    protected SessionState getSessionState() {
        return sessionState.get();
    }
}

package com.ctrip.xpipe.redis.proxy.event;

import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.session.SessionStateChangeEvent;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosed;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnel;
import com.ctrip.xpipe.redis.proxy.tunnel.state.BackendClosed;
import com.ctrip.xpipe.redis.proxy.tunnel.state.FrontendClosed;
import com.ctrip.xpipe.redis.proxy.tunnel.state.TunnelClosed;
import com.ctrip.xpipe.redis.proxy.tunnel.state.TunnelClosing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chen.zhu
 * <p>
 * May 14, 2018
 */
public class SessionClosedEventHandler implements EventHandler {

    private static final Logger logger = LoggerFactory.getLogger(SessionClosedEventHandler.class);

    private Tunnel tunnel;

    private Session session;

    private SessionStateChangeEvent event;

    public SessionClosedEventHandler(Session session, SessionStateChangeEvent event) {
        this.tunnel = session.tunnel();
        this.session = session;
        this.event = event;
    }

    @Override
    public void handle() {
        if(!event.getCurrent().equals(new SessionClosed(null))) {
            return;
        }
        // tunnel-established -> BACKEND/FRONTEND-CLOSED -> tunnel-closing(close other session) -> tunnel-closed
        if(tunnel.getState().equals(new TunnelClosing(null))) {
            tunnel.setState(new TunnelClosed((DefaultTunnel) tunnel));
            return;
        }
        switch (session.getSessionType()) {
            case FRONTEND:
                tunnel.setState(new FrontendClosed((DefaultTunnel) tunnel));
                break;
            case BACKEND:
                tunnel.setState(new BackendClosed((DefaultTunnel) tunnel));
                break;

                default:
                   logger.error("[handle] session type un-defined: {}", session.toString());
                   break;
        }

    }
}

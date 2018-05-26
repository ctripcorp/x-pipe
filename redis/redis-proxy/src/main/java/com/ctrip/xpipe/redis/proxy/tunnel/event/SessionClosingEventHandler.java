package com.ctrip.xpipe.redis.proxy.tunnel.event;

import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.session.SessionStateChangeEvent;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosed;

/**
 * @author chen.zhu
 * <p>
 * May 14, 2018
 */
public class SessionClosingEventHandler extends AbstractSessionEventHandler {

    public SessionClosingEventHandler(Session session, SessionStateChangeEvent event) {
        super(session, event);
    }

    @Override
    protected void doHandle() {
        session.disconnect();
        session.setSessionState(new SessionClosed(session));
    }
}

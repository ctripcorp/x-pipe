package com.ctrip.xpipe.redis.proxy.tunnel.event;


import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.session.SESSION_TYPE;
import com.ctrip.xpipe.redis.proxy.session.SessionStateChangeEvent;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnel;
import com.ctrip.xpipe.redis.proxy.tunnel.state.TunnelEstablished;
import com.ctrip.xpipe.redis.proxy.tunnel.state.TunnelHalfEstablished;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chen.zhu
 * <p>
 * May 14, 2018
 */
public class SessionEstablishedHandler extends AbstractSessionEventHandler {

    private static Logger logger = LoggerFactory.getLogger(SessionEstablishedHandler.class);

    public SessionEstablishedHandler(Session session, SessionStateChangeEvent event) {
        super(session, event);
    }

    @Override
    public void doHandle() {
        Tunnel tunnel = session.tunnel();
        if(!(tunnel.getState() instanceof TunnelHalfEstablished)) {
            logger.info("[doHandle] tunnel state {}, not able transfer to established", tunnel.getState().name());
            return;
        }
        if(session.getSessionType() == SESSION_TYPE.BACKEND) {
            tunnel.setState(new TunnelEstablished((DefaultTunnel) tunnel));
        }
    }
}

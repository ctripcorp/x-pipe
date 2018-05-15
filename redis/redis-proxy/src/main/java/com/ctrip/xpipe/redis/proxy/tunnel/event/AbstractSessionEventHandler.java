package com.ctrip.xpipe.redis.proxy.tunnel.event;

import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.session.SessionStateChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chen.zhu
 * <p>
 * May 14, 2018
 */
public abstract class AbstractSessionEventHandler implements EventHandler {

    private static Logger logger = LoggerFactory.getLogger(AbstractSessionEventHandler.class);

    protected Session session;

    protected SessionStateChangeEvent event;

    public AbstractSessionEventHandler(Session session, SessionStateChangeEvent event) {
        this.session = session;
        this.event = event;
    }

    @Override
    public void handle() {
        logger.info("[handle]session: {}, event: {} - {}", session.getSessionMeta(), event.getPrevious(), event.getCurrent());
        doHandle();
    }

    protected abstract void doHandle();
}

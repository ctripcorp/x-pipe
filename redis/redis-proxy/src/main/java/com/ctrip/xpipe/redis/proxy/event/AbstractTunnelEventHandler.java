package com.ctrip.xpipe.redis.proxy.event;

import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelStateChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chen.zhu
 * <p>
 * May 14, 2018
 */
public abstract class AbstractTunnelEventHandler implements EventHandler {

    private static Logger logger = LoggerFactory.getLogger(AbstractTunnelEventHandler.class);

    protected Tunnel tunnel;

    protected TunnelStateChangeEvent event;

    @Override
    public void handle() {
        logger.info("[handle] handle tunnel: {}, event: {}", tunnel.getTunnelMeta(), event);
        doHandle();
    }

    protected abstract void doHandle();
}

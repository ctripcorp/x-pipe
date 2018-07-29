package com.ctrip.xpipe.redis.proxy.tunnel;

/**
 * @author chen.zhu
 * <p>
 * May 13, 2018
 */
public class TunnelStateChangeEvent {

    private TunnelState previous;

    private TunnelState current;

    public TunnelStateChangeEvent(TunnelState oldState, TunnelState newState) {
        this.previous = oldState;
        this.current = newState;
    }

    public TunnelState getPrevious() {
        return previous;
    }

    public TunnelState getCurrent() {
        return current;
    }
}

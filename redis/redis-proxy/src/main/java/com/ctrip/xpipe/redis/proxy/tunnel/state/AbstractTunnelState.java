package com.ctrip.xpipe.redis.proxy.tunnel.state;

import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnel;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelState;
import com.ctrip.xpipe.utils.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chen.zhu
 * <p>
 * May 12, 2018
 */
public abstract class AbstractTunnelState implements TunnelState {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractTunnelState.class);

    protected DefaultTunnel tunnel;

    public AbstractTunnelState(DefaultTunnel tunnel) {
        this.tunnel = tunnel;
    }

    @Override
    public TunnelState nextAfterSuccess() {
        TunnelState next = doNextAfterSuccess();
        logger.debug("[nextAfterSuccess] current: {}, next: {}", this.name(), next == null ? "null" : next.name());
        return next;
    }

    protected abstract TunnelState doNextAfterSuccess();

    @Override
    public TunnelState nextAfterFail() {
        TunnelState next = doNextAfterFail();
        logger.debug("[nextAfterFail] current: {}, next: {}", this.name(), next == null ? "null" : next.name());
        return next;
    }

    protected abstract TunnelState doNextAfterFail();

    @Override
    public boolean isValidNext(TunnelState tunnelState) {
        return tunnelState.equals(this.nextAfterFail()) ||
                tunnelState.equals(this.nextAfterSuccess());
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }
        if(!(obj instanceof TunnelState)) {
            return false;
        }
        TunnelState other = (TunnelState) obj;
        return this.name().equals(other.name());
    }

    @Override
    public String toString() {
        return name();
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(tunnel);
    }
}

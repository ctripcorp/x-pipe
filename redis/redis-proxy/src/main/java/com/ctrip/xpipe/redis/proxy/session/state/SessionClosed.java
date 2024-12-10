package com.ctrip.xpipe.redis.proxy.session.state;

import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.exception.WriteToClosedSessionException;
import com.ctrip.xpipe.redis.proxy.session.SessionState;
import io.netty.buffer.ByteBuf;

/**
 * @author chen.zhu
 * <p>
 * May 13, 2018
 */
public class SessionClosed extends AbstractSessionState {

    public SessionClosed(Session session) {
        super(session);
    }

    @Override
    protected SessionState doNextAfterSuccess() {
        return this;
    }

    @Override
    protected SessionState doNextAfterFail() {
        return this;
    }

    @Override
    public void tryWrite(ByteBuf byteBuf) {
        throw new WriteToClosedSessionException("Session's been closed");
    }

    @Override
    public String name() {
        return "Session-Closed";
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}

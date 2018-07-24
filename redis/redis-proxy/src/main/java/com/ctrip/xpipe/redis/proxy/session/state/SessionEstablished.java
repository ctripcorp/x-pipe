package com.ctrip.xpipe.redis.proxy.session.state;

import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.session.AbstractSession;
import com.ctrip.xpipe.redis.proxy.session.SessionState;
import io.netty.buffer.ByteBuf;

/**
 * @author chen.zhu
 * <p>
 * May 13, 2018
 */
public class SessionEstablished extends AbstractSessionState {

    public SessionEstablished(Session session) {
        super(session);
    }

    @Override
    protected SessionState doNextAfterSuccess() {
        return this;
    }

    @Override
    protected SessionState doNextAfterFail() {
        return new SessionClosing(session);
    }

    @Override
    public void tryWrite(ByteBuf byteBuf) {
        ((AbstractSession)session).doWrite(byteBuf);
    }

    @Override
    public String name() {
        return "Session-Established";
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean isValidNext(SessionState sessionState) {
        return (sessionState instanceof SessionClosed) || super.isValidNext(sessionState);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}

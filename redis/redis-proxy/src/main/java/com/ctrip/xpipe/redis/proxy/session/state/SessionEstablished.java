package com.ctrip.xpipe.redis.proxy.session.state;

import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.session.AbstractSession;
import com.ctrip.xpipe.redis.proxy.session.SessionState;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;

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
    public ChannelFuture tryWrite(ByteBuf byteBuf) {
        return ((AbstractSession)session).doWrite(byteBuf);
    }

    @Override
    public void disconnect() {
        throw new UnsupportedOperationException("Session disconnect when Session-Closing state");
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
    public boolean isValidNext(SessionState sessionState) {
        return (sessionState instanceof SessionClosed) || super.isValidNext(sessionState);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}

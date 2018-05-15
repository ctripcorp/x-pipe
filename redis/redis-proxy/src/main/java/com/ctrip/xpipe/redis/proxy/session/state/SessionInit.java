package com.ctrip.xpipe.redis.proxy.session.state;

import com.ctrip.xpipe.redis.proxy.exception.WriteWhenSessionInitException;
import com.ctrip.xpipe.redis.proxy.session.DefaultSession;
import com.ctrip.xpipe.redis.proxy.session.SessionState;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;

/**
 * @author chen.zhu
 * <p>
 * May 13, 2018
 */
public class SessionInit extends AbstractSessionState {

    public SessionInit(DefaultSession session) {
        super(session);
    }

    @Override
    protected SessionState doNextAfterSuccess() {
        return new SessionEstablished(session);
    }

    @Override
    protected SessionState doNextAfterFail() {
        return new SessionClosed(session);
    }

    @Override
    public ChannelFuture tryWrite(ByteBuf byteBuf) {
        throw new WriteWhenSessionInitException("Cannot write when session initializing");
    }

    @Override
    public ChannelFuture connect() {
        logger.info("[connect] Session connect");
        return session.tryConnect();
    }

    @Override
    public void disconnect() {
        throw new UnsupportedOperationException("disconnect only allowed in session closing");
    }

    @Override
    public String name() {
        return "Session-Init";
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}

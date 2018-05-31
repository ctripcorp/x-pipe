package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.redis.proxy.event.EventHandler;
import com.ctrip.xpipe.redis.proxy.model.SessionMeta;
import com.ctrip.xpipe.redis.proxy.session.SESSION_TYPE;
import com.ctrip.xpipe.redis.proxy.session.SessionEventHandler;
import com.ctrip.xpipe.redis.proxy.session.SessionState;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public interface Session extends Lifecycle, Releasable, Observable {

    Tunnel tunnel();

    void disconnect();

    void addSessionEventHandler(SessionEventHandler handler);

    void makeReadable();

    void makeUnReadable();

    ChannelFuture tryWrite(ByteBuf byteBuf);

    void setSessionState(SessionState sessionState);

    SessionState getSessionState();

    boolean isReleasable();

    Channel getChannel();

    SessionMeta getSessionMeta();

    SESSION_TYPE getSessionType();

    @Override
    void release();

    void setWritableState(SessionWritableState state);

    enum SessionWritableState {
        WRITABLE, UNWRITABLE
    }
}

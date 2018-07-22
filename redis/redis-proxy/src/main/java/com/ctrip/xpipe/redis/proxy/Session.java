package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.redis.proxy.model.SessionMeta;
import com.ctrip.xpipe.redis.proxy.session.SESSION_TYPE;
import com.ctrip.xpipe.redis.proxy.session.SessionEventHandler;
import com.ctrip.xpipe.redis.proxy.session.SessionState;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public interface Session extends Lifecycle, Releasable, Observable {

    Tunnel tunnel();

    Channel getChannel();

    void makeReadable();

    void makeUnReadable();

    boolean logRead();

    boolean logWrite();

    void markReadLoggability(boolean isLoggable);

    void markWriteLoggability(boolean isLoggable);

    SessionMeta getSessionMeta();

    SESSION_TYPE getSessionType();

    SessionState getSessionState();

    void tryWrite(ByteBuf byteBuf);

    void setWritableState(SessionWritableState state);

    void addSessionEventHandler(SessionEventHandler handler);

    enum SessionWritableState {
        WRITABLE, UNWRITABLE
    }

    // no exception throw for release
    @Override
    void release();
}

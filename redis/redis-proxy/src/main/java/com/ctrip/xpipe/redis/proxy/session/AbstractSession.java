package com.ctrip.xpipe.redis.proxy.session;

import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpoint;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.model.SessionMeta;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosed;
import com.ctrip.xpipe.utils.ChannelUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author chen.zhu
 * <p>
 * May 24, 2018
 */
public abstract class AbstractSession extends AbstractLifecycleObservable implements Session {

    protected final static Logger logger = LoggerFactory.getLogger(AbstractSession.class);

    protected final static String SESSION_STATE_CHANGE = "Session.State.Change";

    protected ProxyEndpoint endpoint;

    protected Channel channel;

    private Tunnel tunnel;

    protected long trafficReportIntervalMillis;

    private List<SessionEventHandler> handlers = Lists.newArrayList();

    private volatile SessionWritableState writableState = SessionWritableState.WRITABLE;

    private AtomicBoolean logRead = new AtomicBoolean(false);

    private AtomicBoolean logWrite = new AtomicBoolean(false);

    protected AbstractSession(Tunnel tunnel, long trafficReportIntervalMillis) {
        this.tunnel = tunnel;
        this.trafficReportIntervalMillis = trafficReportIntervalMillis;
    }

    @Override
    public Tunnel tunnel() {
        return tunnel;
    }

    @Override
    public void addSessionEventHandler(SessionEventHandler handler) {
        handlers.add(handler);
    }

    @Override
    public void makeReadable() {
        ChannelUtil.triggerChannelAutoRead(getChannel());
    }

    @Override
    public void makeUnReadable() {
        ChannelUtil.closeChannelAutoRead(getChannel());
    }

    @Override
    public void setWritableState(SessionWritableState state) {
        if(state.equals(writableState)) {
            return;
        }
        writableState = state;
        switch (state) {
            case WRITABLE:
                onSessionWritable();
                break;
            case UNWRITABLE:
                onSessionNotWritable();
                break;

                default: break;
        }
    }

    @Override
    public boolean logRead() {
        return logRead.get();
    }

    @Override
    public boolean logWrite() {
        return logWrite.get();
    }

    @Override
    public void markReadLoggability(boolean isLoggable) {
        logRead.set(isLoggable);
    }

    @Override
    public void markWriteLoggability(boolean isLoggable) {
        logWrite.set(isLoggable);
    }

    protected void onSessionInit() {
        for(SessionEventHandler handler : handlers) {
            handler.onInit();
        }
    }

    protected void onSessionEstablished() {
        for(SessionEventHandler handler : handlers) {
            handler.onEstablished();
        }
    }

    private void onSessionWritable() {
        for(SessionEventHandler handler : handlers) {
            handler.onWritable();
        }
    }

    private void onSessionNotWritable() {
        for(SessionEventHandler handler : handlers) {
            handler.onNotWritable();
        }
    }

    @Override
    public void tryWrite(ByteBuf byteBuf) {
        getSessionState().tryWrite(byteBuf);
    }

    public void doWrite(ByteBuf byteBuf) {
        if(logger.isDebugEnabled()) {
            logger.debug("[doWrite] {}: {}", getSessionType(), ByteBufUtil.prettyHexDump(byteBuf));
        }
        getChannel().writeAndFlush(byteBuf.retain(), getChannel().voidPromise());
    }

    protected void setSessionState(SessionState newState) {
        if(!getSessionState().isValidNext(newState)) {
            logger.error("[setSessionState] Set state failed, state relationship not match, old: {}, new: {}",
                    getSessionState(), newState.name());
            return;
        }
        doSetSessionState(newState);
    }

    protected abstract void doSetSessionState(SessionState newState);

    @Override
    public Channel getChannel() {
        return channel;
    }

    @Override
    public SessionMeta getSessionMeta() {
        try {
            return new SessionMeta(this, endpoint, getSessionState());
        } catch (Exception e) {
            return new SessionMeta(getSessionType().name(), "unknown", "unknown", getSessionState().name());
        }
    }

    @Override
    public SESSION_TYPE getSessionType() {
        return null;
    }

    @Override
    public void release() {
        if(channel != null) {
            channel.close();
        }
        setSessionState(new SessionClosed(this));
    }

    @VisibleForTesting
    public void setChannel(Channel channel) {
        AbstractSession.this.channel = channel;
    }

}

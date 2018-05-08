package com.ctrip.xpipe.redis.proxy.session;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpoint;
import com.ctrip.xpipe.redis.proxy.Courier;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.model.SessionMeta;
import com.dianping.cat.Cat;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */
public class DefaultSession extends AbstractLifecycle implements Session {

    private static final String STATE_CHANGE = "Session.State.Change";

    private ProxyEndpoint endpoint;

    private Channel channel;

    private Tunnel tunnel;

    private Courier courier;

    private AtomicReference<SessionState> sessionState;

    private SESSION_TYPE type;

    public DefaultSession(Tunnel tunnel, SESSION_TYPE type, ProxyEndpoint endpoint) {
        this.endpoint = endpoint;
        this.tunnel = tunnel;
        this.type = type;
    }

    public DefaultSession(Tunnel tunnel, SESSION_TYPE type, Channel channel) {
        this.channel = channel;
        this.endpoint = new DefaultProxyEndpoint((InetSocketAddress) channel.remoteAddress());
        this.tunnel = tunnel;
        this.type = type;
    }

    @Override
    public Tunnel tunnel() {
        return tunnel;
    }

    @Override
    public ChannelFuture tryConnect() {
        return sessionState.get().tryConnect();
    }

    @Override
    public void disconnect() {
        sessionState.get().disconnect();
    }

    @Override
    public ChannelFuture tryWrite(ByteBuf byteBuf) {
        return sessionState.get().tryWrite(byteBuf);
    }

    @Override
    public void setCourier(Courier courier) {
        this.courier = courier;
    }

    @Override
    public void setEndpoint(ProxyEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    @Override
    public Channel getChannel() {
        return channel;
    }

    @Override
    public SESSION_TYPE getSessionType() {
        return type;
    }

    @Override
    public SessionMeta getSessionMeta() {
        return null;
    }

    @Override
    public void setSessionState(SessionState newState) {
        SessionState oldState = this.sessionState.getAndSet(newState);
        if(oldState.equals(newState)) {
            logger.info("[setSessionState] already session state: {}", oldState);
        } else {
            Cat.logEvent(STATE_CHANGE, String.format("Session: %s, %s -> %s", getSessionMeta(),
                    oldState.toString(), newState.toString()));
        }
    }
}

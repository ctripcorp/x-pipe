package com.ctrip.xpipe.redis.proxy.session;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.observer.AbstractObservable;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.handler.NettySslHandlerFactory;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.buffer.BufferStore;
import com.ctrip.xpipe.redis.proxy.buffer.SimpleBlockingQueueBufferStore;
import com.ctrip.xpipe.redis.proxy.handler.TunnelNettyHandler;
import com.ctrip.xpipe.redis.proxy.handler.TunnelTrafficReporter;
import com.ctrip.xpipe.redis.proxy.model.SessionMeta;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosed;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosing;
import com.ctrip.xpipe.redis.proxy.session.state.SessionEstablished;
import com.ctrip.xpipe.redis.proxy.session.state.SessionInit;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */
public class DefaultSession extends AbstractObservable implements Session {

    private static final String STATE_CHANGE = "Session.State.Change";

    private ProxyEndpoint endpoint;

    private Channel channel;

    private Tunnel tunnel;

    private AtomicReference<SessionState> sessionState;

    private SESSION_TYPE type;

    private EventLoopGroup nioEventLoopGroup;

    private NettySslHandlerFactory sslHandlerFactory;

    private long trafficReportIntervalMillis;

    private ChannelFuture connectionFuture;

    private BufferStore bufferStore; // try to store bytebuf when traffic is not smooth

    public DefaultSession(Tunnel tunnel, SESSION_TYPE type, ProxyEndpoint endpoint,
                          NettySslHandlerFactory factory, long trafficReportIntervalMillis) {
        this.endpoint = endpoint;
        this.tunnel = tunnel;
        this.type = type;
        this.sslHandlerFactory = factory;
        this.trafficReportIntervalMillis = trafficReportIntervalMillis;
        this.sessionState = new AtomicReference<>(new SessionInit(this));
        this.bufferStore = new SimpleBlockingQueueBufferStore(this);
    }

    public DefaultSession(Tunnel tunnel, SESSION_TYPE type, Channel channel, NettySslHandlerFactory factory,
                          long trafficReportIntervalMillis) {
        this.channel = channel;
        this.endpoint = new DefaultProxyEndpoint((InetSocketAddress) channel.remoteAddress());
        this.tunnel = tunnel;
        this.type = type;
        this.sslHandlerFactory = factory;
        this.trafficReportIntervalMillis = trafficReportIntervalMillis;
        this.sessionState = new AtomicReference<>(new SessionEstablished(this));
        this.bufferStore = new SimpleBlockingQueueBufferStore(this);
    }

    @Override
    public Tunnel tunnel() {
        return tunnel;
    }

    @Override
    public void forward(ByteBuf message) {
        tunnel().forward(message, this);
    }

    @Override
    public void cache(ByteBuf message) {
        if(bufferStore != null) {
            message.retain();
            bufferStore.offer(message);
        }
    }

    @Override
    public ChannelFuture connect() {
        return sessionState.get().connect();
    }

    public ChannelFuture tryConnect() {

        connectionFuture = initChannel();
        connectionFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if(future.isSuccess()) {
                    setChannel(future.channel());
                    setSessionState(sessionState.get().nextAfterSuccess());
                    logger.info("[tryConnect] session established: {}", getSessionMeta());

                } else {
                    logger.error("[tryConnect] fail to connect: {}", getSessionMeta(), future.cause());
                    setSessionState(sessionState.get().nextAfterFail());
                }
            }
        });
        return connectionFuture;
    }

    private ChannelFuture initChannel() {
        Bootstrap b = new Bootstrap();
        b.group(nioEventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        if(endpoint.isSslEnabled()) {
                            p.addLast(sslHandlerFactory.createSslHandler());
                        }
                        p.addLast(new LoggingHandler(LogLevel.DEBUG));
                        p.addLast(new TunnelNettyHandler(tunnel()));
                        p.addLast(new TunnelTrafficReporter(trafficReportIntervalMillis, tunnel()));
                    }
                });
        return b.connect(endpoint.getHost(), endpoint.getPort());
    }

    @Override
    public void disconnect() {
        sessionState.get().disconnect();
    }

    public void doDisconnect() {
        try {
            release();
        } catch (Exception e) {
            logger.error("[doDisconnect] session: {}", getSessionMeta(), e);
        }
    }

    @Override
    public ChannelFuture tryWrite(ByteBuf byteBuf) {
        return sessionState.get().tryWrite(byteBuf);
    }

    public ChannelFuture doWrite(ByteBuf byteBuf) {
        logger.info("[doWrite] {}: {}", getSessionType(), byteBuf.toString(Charset.defaultCharset()));
        ChannelFuture future = getChannel().writeAndFlush(byteBuf);
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if(!future.isSuccess()) {
                    logger.error("[doWrite] write failed: {}", future.cause());
                    setSessionState(sessionState.get().nextAfterFail());
                }
            }
        });
        return future;
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
        return new SessionMeta(this, endpoint, this.sessionState.get());
    }

    @Override
    public synchronized void setSessionState(SessionState newState) {
        if(!sessionState.get().isValidNext(newState)) {
            logger.error("[setStasetSessionStatete] Set state failed, state relationship not match, old: {}, new: {}",
                    sessionState.get(), newState);
            return;
        }
        SessionState oldState = this.sessionState.getAndSet(newState);
        if(oldState.equals(newState)) {
            logger.info("[setSessionState] already session state: {}", oldState);
        } else {
            logger.info("[setSessionState] Session state change from {} to {} ({})", oldState, newState, getSessionMeta());
            EventMonitor.DEFAULT.logEvent(STATE_CHANGE, String.format("Session: %s, %s -> %s", getSessionMeta(),
                    oldState.toString(), newState.toString()));
            notifyObservers(new SessionStateChangeEvent(oldState, newState));
        }
    }

    @Override
    public void release() throws Exception {
        if(sessionState.get().equals(new SessionClosed(this))) {
            logger.info("[release] session closed, no need to release");
            return;
        }
        logger.info("[release] {}", getSessionMeta());
        // try to set to closing, in case that release has been called directly
        setSessionState(new SessionClosing(this));
        if(bufferStore != null) {
            bufferStore.release();
            bufferStore = null;
        }
        if(channel != null) {
            channel.close().sync();
            channel = null;
        }
        if(connectionFuture != null && connectionFuture.channel().isActive()) {
            connectionFuture.channel().close().sync();
            connectionFuture = null;
        }
        setSessionState(new SessionClosed(this));
    }

    public DefaultSession setNioEventLoopGroup(EventLoopGroup nioEventLoopGroup) {
        this.nioEventLoopGroup = nioEventLoopGroup;
        return this;
    }

}

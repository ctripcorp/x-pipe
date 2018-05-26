package com.ctrip.xpipe.redis.proxy.session;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointSelector;
import com.ctrip.xpipe.redis.core.proxy.handler.NettySslHandlerFactory;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.exception.ResourceIncorrectException;
import com.ctrip.xpipe.redis.proxy.handler.BackendSessionHandler;
import com.ctrip.xpipe.redis.proxy.handler.TunnelTrafficReporter;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosing;
import com.ctrip.xpipe.redis.proxy.session.state.SessionEstablished;
import com.ctrip.xpipe.redis.proxy.session.state.SessionInit;
import com.ctrip.xpipe.redis.proxy.tunnel.event.EventHandler;
import com.google.common.collect.Lists;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author chen.zhu
 * <p>
 * May 24, 2018
 */
public class DefaultBackendSession extends AbstractSession implements BackendSession {

    private ProxyEndpointSelector selector;

    private ByteBuf sendAfterProtocol = null;

    private EventLoopGroup nioEventLoopGroup;

    private NettySslHandlerFactory sslHandlerFactory;

    private AtomicReference<SessionState> sessionState;

    private List<EventHandler> channelEstablishedHandlers = Lists.newArrayList();

    public DefaultBackendSession(Tunnel tunnel, long trafficReportIntervalMillis, ProxyEndpointSelector selector,
                                 EventLoopGroup eventLoopGroup, NettySslHandlerFactory sslHandlerFactory) {
        super(tunnel, trafficReportIntervalMillis);
        this.selector = selector;
        this.nioEventLoopGroup = eventLoopGroup;
        this.sslHandlerFactory = sslHandlerFactory;
        this.sessionState = new AtomicReference<>(new SessionInit(this));
    }

    private void connect() {
        if(!(sessionState.get() instanceof SessionInit)) {
            logger.info("[connect] not session init state, quit");
            return;
        }
        if(selector.selectCounts() == selector.getCandidates().size()) {
            // Retry times up, close session
            setSessionState(new SessionClosing(this));
            throw new ResourceIncorrectException("No candidates are available");
        }

        ProxyEndpoint endpoint = this.endpoint = selector.nextHop();
        ChannelFuture connectionFuture = initChannel(endpoint);
        connectionFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if(future.isSuccess()) {
                    onChannelEstablished(future.channel());
                } else {
                    logger.error("[tryConnect] fail to connect: {}", getSessionMeta(), future.cause());
                    future.channel().eventLoop().schedule(()->connect(), 1, TimeUnit.MILLISECONDS);
                }
            }
        });
    }

    private ChannelFuture initChannel(ProxyEndpoint endpoint) {
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
                        p.addLast(new BackendSessionHandler(tunnel()));
                        p.addLast(new TunnelTrafficReporter(trafficReportIntervalMillis, DefaultBackendSession.this));
                    }
                });
        return b.connect(endpoint.getHost(), endpoint.getPort());
    }

    @Override
    public void sendImmdiateAfterProtocol(ByteBuf byteBuf) throws Exception {
        if(sendAfterProtocol == null) {
            sendAfterProtocol = byteBuf.retain();
            return;
        }
        throw new IllegalAccessException("ByteBuf send after protocol has been valued");
    }

    @Override
    public ProxyEndpoint getEndpoint() {
        return super.endpoint;
    }

    @Override
    public void registerChannelEstablishedHandler(EventHandler handler) {
        channelEstablishedHandlers.add(handler);
    }

    protected void onChannelEstablished(Channel channel) {
        setChannel(channel);

        ChannelFuture future = null;
        if(endpoint.isProxyProtocolSupported()) {
            future = getChannel().writeAndFlush(tunnel().getProxyProtocol().output());
        }
        if(sendAfterProtocol != null) {
            future = getChannel().writeAndFlush(sendAfterProtocol);
        }
        setSessionState(new SessionEstablished(DefaultBackendSession.this));

        if(future != null) {
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    executeChannelEstablishHandlers();
                }
            });
        } else {
            executeChannelEstablishHandlers();
        }

    }

    private void executeChannelEstablishHandlers() {
        for(EventHandler handler : channelEstablishedHandlers) {
            handler.handle();
        }
    }

    @Override
    protected void doStart() throws Exception {
        connect();
        super.doStart();
    }

    @Override
    protected void doSetSessionState(SessionState newState) {
        SessionState oldState = this.sessionState.getAndSet(newState);
        if(oldState.equals(newState)) {
            logger.debug("[setSessionState] already session state: {}", oldState);
        } else {
            logger.info("[setSessionState] Session state change from {} to {} ({})", oldState, newState, getSessionMeta());
            EventMonitor.DEFAULT.logEvent(SESSION_STATE_CHANGE, String.format("Session: %s, %s -> %s", getSessionMeta(),
                    oldState.toString(), newState.toString()));
            notifyObservers(new SessionStateChangeEvent(oldState, newState));
        }
    }

    @Override
    public SESSION_TYPE getSessionType() {
        return SESSION_TYPE.BACKEND;
    }

    public SessionState getSessionState() {
        return sessionState.get();
    }

}

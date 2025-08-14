package com.ctrip.xpipe.redis.proxy.session;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointSelector;
import com.ctrip.xpipe.redis.core.proxy.handler.NettySslHandlerFactory;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.redis.proxy.handler.BackendSessionHandler;
import com.ctrip.xpipe.redis.proxy.handler.SessionTrafficReporter;
import com.ctrip.xpipe.redis.proxy.monitor.SessionMonitor;
import com.ctrip.xpipe.redis.proxy.resource.GlobalTrafficControlManager;
import com.ctrip.xpipe.redis.proxy.resource.ResourceManager;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosed;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosing;
import com.ctrip.xpipe.redis.proxy.session.state.SessionEstablished;
import com.ctrip.xpipe.redis.proxy.session.state.SessionInit;
import com.ctrip.xpipe.utils.ChannelUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.ReferenceCountUtil;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.ctrip.xpipe.redis.proxy.DefaultProxyServer.WRITE_HIGH_WATER_MARK;
import static com.ctrip.xpipe.redis.proxy.DefaultProxyServer.WRITE_LOW_WATER_MARK;

/**
 * @author chen.zhu
 * <p>
 * May 24, 2018
 */
public class DefaultBackendSession extends AbstractSession implements BackendSession {
    private static LoggingHandler loggingHandler = new LoggingHandler(LogLevel.DEBUG);

    @VisibleForTesting
    static final String BACKEND_SESSION_HANDLER = "backendSessionHandler";

    @VisibleForTesting
    static final String BACKEND_COMPRESS_DECODER = "backendCompressDecoder";

    @VisibleForTesting
    static final String BACKEND_COMPRESS_ENCODER = "backendCompressEncoder";

    private ProxyEndpointSelector selector;

    private volatile Queue<ByteBuf> sendAfterProtocol = new LinkedList<>();

    private EventLoopGroup nioEventLoopGroup;

    private NettySslHandlerFactory sslHandlerFactory;

    private AtomicReference<SessionState> sessionState;

    private ResourceManager resourceManager;

    public DefaultBackendSession(Tunnel tunnel, EventLoopGroup eventLoopGroup, long trafficReportIntervalMillis,
                                 ResourceManager resourceManager) {
        super(tunnel, trafficReportIntervalMillis);
        this.resourceManager = resourceManager;
        this.selector = resourceManager.createProxyEndpointSelector(tunnel.getProxyProtocol());
        this.nioEventLoopGroup = eventLoopGroup;
        this.sslHandlerFactory = resourceManager.getClientSslHandlerFactory();
        this.sessionState = new AtomicReference<>(new SessionInit(this));
    }

    private void connect() {
        if(!(sessionState.get() instanceof SessionInit)) {
            logger.info("[connect] not session init state, quit");
            return;
        }

        ChannelFuture connectionFuture;
        try {
            this.endpoint = selector.nextHop();
            connectionFuture = initChannel(endpoint);
        } catch (Throwable th) {
            setSessionState(new SessionClosed(this));
            throw th;
        }

        connectionFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if(future.isSuccess()) {
                    try {
                        // may OOM on ZstdEncoder init
                        onChannelEstablished(future.channel());
                    } catch (Throwable th) {
                        logger.info("[tryConnect][connect success, but init fail] {}", getSessionMeta(), th);
                        future.channel().close();
                        setSessionState(new SessionClosed(DefaultBackendSession.this));
                    }
                } else if (null != future.channel() && future.channel().isRegistered()) {
                    logger.error("[tryConnect][connect fail, retry] {}", getSessionMeta(), future.cause());
                    future.channel().eventLoop()
                            .schedule(new AbstractExceptionLogTask() {
                                @Override
                                protected void doRun() throws Exception {
                                    connect();
                                }
                            }, selector.selectCounts(), TimeUnit.MILLISECONDS);
                } else {
                    logger.error("[tryConnect][channel fail, no retry] {}", getSessionMeta(), future.cause());
                    setSessionState(new SessionClosed(DefaultBackendSession.this));
                }
            }
        });
    }

    private ChannelFuture initChannel(ProxyEndpoint endpoint) {
        Bootstrap b = new Bootstrap();
        ProxyConfig config = resourceManager.getProxyConfig();
        b.group(nioEventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 7 * 1000) //7 sec timeout, to avoid forever waiting
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(WRITE_LOW_WATER_MARK, WRITE_HIGH_WATER_MARK))
                .option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(config.getFixedRecvBufferSize()))
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        if(endpoint.isSslEnabled()) {
                            p.addLast(sslHandlerFactory.createSslHandler(ch));
                        }
                        p.addLast(loggingHandler);
                        
                        // Add traffic control handler for cross-region proxy tunnels
                        if (tunnel().getProxyProtocol().isNextHopProxy()) {
                            GlobalTrafficControlManager trafficControlManager = resourceManager.getGlobalTrafficControlManager();
                            if (trafficControlManager.isTrafficControlEnabled()) {
                                io.netty.handler.traffic.GlobalTrafficShapingHandler trafficHandler = trafficControlManager.getTrafficShapingHandler();
                                if (trafficHandler != null) {
                                    p.addLast("globalTrafficShaping", trafficHandler);
                                    logger.debug("[initChannel] Added global traffic shaping handler for cross-region tunnel: {}", tunnel().identity());
                                }
                            }
                        }
                        
                        p.addLast(new SessionTrafficReporter(trafficReportIntervalMillis, config::shouldReportTraffic, DefaultBackendSession.this));
                        p.addLast(BACKEND_SESSION_HANDLER, new BackendSessionHandler(tunnel()));
                    }
                });
        return b.connect(endpoint.getHost(), endpoint.getPort());
    }

    @Override
    public synchronized void sendAfterProtocol(ByteBuf byteBuf) throws Exception {
        if (null == sendAfterProtocol) {
            if (isClosed()) {
                logger.info("[sendAfterProtocol][already closed] skip");
            } else {
                logger.info("[sendAfterProtocol][unexpected fail] close");
                release();
            }
        } else {
            if (logger.isDebugEnabled()) logger.debug("[sendAfterProtocol][{}] {}", getSessionId(), ByteBufUtil.prettyHexDump(byteBuf));
            sendAfterProtocol.offer(byteBuf.retain());
        }
    }

    protected void onChannelEstablished(Channel channel) {
        setChannel(channel);

        logger.debug("[onChannelEstablished] {}", getSessionMeta());
        if(endpoint.isProxyProtocolSupported()) {
            getChannel().writeAndFlush(tunnel().getProxyProtocol().output());
        }
        ProxyConfig config = resourceManager.getProxyConfig();
        if(config.isCompressEnabled() && tunnel().getProxyProtocol().isNextHopProxy()) {
            logger.info("Backend compress codec installed: {}", ChannelUtil.getDesc(channel));
            channel.pipeline().addBefore(BACKEND_SESSION_HANDLER, BACKEND_COMPRESS_DECODER, config.getCompressDecoder());
            channel.pipeline().addBefore(BACKEND_SESSION_HANDLER, BACKEND_COMPRESS_ENCODER, config.getCompressEncoder());
        }
        if(sendAfterProtocol != null) {
            while(!sendAfterProtocol.isEmpty()) {
                getChannel().writeAndFlush(sendAfterProtocol.poll());
            }
            sendAfterProtocol = null;
        }
        setSessionState(new SessionEstablished(DefaultBackendSession.this));
        SessionState currentSessionState = getSessionState();
        if (currentSessionState instanceof SessionClosing || currentSessionState instanceof SessionClosed) {
            logger.info("[onChannelEstablished][but session {}] {}", currentSessionState, getSessionMeta());
            release();
        } else {
            onSessionEstablished();
        }
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        onSessionInit();
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        connect();
    }

    @Override
    protected void doSetSessionState(SessionState newState) {
        SessionState oldState = this.sessionState.getAndSet(newState);
        if(oldState.equals(newState)) {
            logger.debug("[setSessionState][Backend] already session state: {}", oldState);
        } else {
            logger.info("[setSessionState][Backend] Session state change from {} to {} ({})", oldState, newState, getSessionMeta());
            EventMonitor.DEFAULT.logEvent(SESSION_STATE_CHANGE, String.format("[Backend]%s->%s", oldState.toString(), newState.toString()),
                    Collections.singletonMap("channel", ChannelUtil.getDesc(getChannel())));
            if (isClosed()) {
                releaseByteBufSendAfterProtocol();
            }
            notifyObservers(new SessionStateChangeEvent(oldState, newState));
        }
    }



    private void releaseByteBufSendAfterProtocol() {
        if (null == sendAfterProtocol) return;

        synchronized (this) {
            if (null == sendAfterProtocol) return;
            logger.info("[release][{}] release sendAfterProtocol", getSessionId());
            while(!sendAfterProtocol.isEmpty()) {
                ByteBuf byteBuf = sendAfterProtocol.poll();
                ReferenceCountUtil.release(byteBuf);
            }
            sendAfterProtocol = null;
        }
    }

    @Override
    public SESSION_TYPE getSessionType() {
        return SESSION_TYPE.BACKEND;
    }

    public SessionState getSessionState() {
        return sessionState.get();
    }

    @Override
    public SessionMonitor getSessionMonitor() {
        return tunnel().getTunnelMonitor().getBackendSessionMonitor();
    }

    @VisibleForTesting
    protected void setSslHandlerFactory(NettySslHandlerFactory factory) {
        this.sslHandlerFactory = factory;
    }

    @VisibleForTesting
    protected void setNioEventLoopGroup(EventLoopGroup group) {
        this.nioEventLoopGroup = group;
    }

}

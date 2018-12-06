package com.ctrip.xpipe.redis.proxy.session;

import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.redis.proxy.handler.BackendSessionHandler;
import com.ctrip.xpipe.redis.proxy.resource.ResourceManager;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosed;
import com.ctrip.xpipe.redis.proxy.session.state.SessionInit;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.socksx.v5.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.ctrip.xpipe.redis.proxy.DefaultProxyServer.WRITE_HIGH_WATER_MARK;
import static com.ctrip.xpipe.redis.proxy.DefaultProxyServer.WRITE_LOW_WATER_MARK;

public class Socks5BackEndSession extends DefaultBackendSession {

    private ProxyEndpoint socks5EndPoint;

    public Socks5BackEndSession(Tunnel tunnel, EventLoopGroup eventLoopGroup, long trafficReportIntervalMillis,
                                ResourceManager resourceManager, ProxyEndpoint socks5EndPoint) {
        super(tunnel, eventLoopGroup, trafficReportIntervalMillis, resourceManager);
        this.socks5EndPoint = socks5EndPoint;
    }

    @Override
    protected void doStart() throws Exception {
        connect();
    }

    protected void connect() {
        if (!(sessionState.get() instanceof SessionInit)) {
            logger.info("[connect] not session init state, quit");
            return;
        }

        try {
            this.endpoint = selector.nextHop();
        } catch (Exception e) {
            setSessionState(new SessionClosed(this));
            logger.error("[connect] select nextHop error", e);
            throw e;
        }

        ChannelFuture connectionFuture = initChannel(endpoint);
        connectionFuture.addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                logger.error("[tryConnect] fail to connect: {}, {}", getSessionMeta(), future.cause());
                future.channel().eventLoop()
                        .schedule(this::connect, selector.selectCounts(), TimeUnit.MILLISECONDS);
            }
        });
    }

    /***
     * 转为了同步方法。。。。
     * @param endpoint
     * @return
     */
    protected ChannelFuture initChannel(ProxyEndpoint endpoint) {
        Bootstrap b = new Bootstrap();
        ProxyConfig config = resourceManager.getProxyConfig();
        CountDownLatch cdl = new CountDownLatch(1);
        b.group(nioEventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10 * 1000) //10 sec timeout, to avoid forever waiting
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(WRITE_LOW_WATER_MARK, WRITE_HIGH_WATER_MARK))
                .option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(config.getFixedRecvBufferSize()))
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        List<ChannelHandler> toAdd = new ArrayList<>();
                        if (endpoint.isSslEnabled()) {
                            toAdd.add(sslHandlerFactory.createSslHandler(ch));
                        }
                        toAdd.add(new LoggingHandler(LogLevel.DEBUG));
                        toAdd.add(new BackendSessionHandler(tunnel()));

                        addSocks5Handlers(p, endpoint, cdl, toAdd);
                    }
                });
        return b.connect(socks5EndPoint.getHost(), socks5EndPoint.getPort());
    }

    private void addSocks5Handlers(ChannelPipeline p, ProxyEndpoint endpoint, CountDownLatch cdl, List<ChannelHandler> toAdd) {
        p.addLast("1", Socks5ClientEncoder.DEFAULT)
                .addLast("2", new Socks5InitialResponseDecoder())
                .addLast("3", new Socks5InitialResponseHandler(endpoint))
                .addLast("6", new Socks5CommandResponseDecoder())
                .addLast("7", new Socks5CommandResponseHandler(toAdd));
    }


    public static class Socks5InitialResponseHandler extends SimpleChannelInboundHandler<DefaultSocks5InitialResponse> {
        private ProxyEndpoint endpoint;

        public Socks5InitialResponseHandler(ProxyEndpoint endpoint) {
            this.endpoint = endpoint;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, DefaultSocks5InitialResponse msg) throws Exception {
            if (msg.authMethod().equals(Socks5AuthMethod.NO_AUTH)) {
                Socks5AddressType type;
                if (isIpv4(endpoint.getHost())) {
                    type = Socks5AddressType.IPv4;
                } else {
                    type = Socks5AddressType.DOMAIN;
                }
                Socks5CommandRequest commandRequest = new DefaultSocks5CommandRequest(Socks5CommandType.CONNECT, type, endpoint.getHost(), endpoint.getPort());
                ctx.writeAndFlush(commandRequest);
            } else {
                ctx.close();
            }
        }

        private boolean isIpv4(String host) {
            String ip = "^(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|[1-9])\\."
                    + "(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
                    + "(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
                    + "(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)$";
            Pattern pattern = Pattern.compile(ip);
            Matcher matcher = pattern.matcher(host);
            return matcher.matches();
        }
    }

    public class Socks5CommandResponseHandler extends SimpleChannelInboundHandler<DefaultSocks5CommandResponse> {
        private List<ChannelHandler> toAdd;

        public Socks5CommandResponseHandler(List<ChannelHandler> toAdd) {
            this.toAdd = toAdd;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, DefaultSocks5CommandResponse msg) throws Exception {
            if (msg.status().equals(Socks5CommandStatus.SUCCESS)) {
                ctx.pipeline().remove("1");
                ctx.pipeline().remove("2");
                ctx.pipeline().remove("3");
                ctx.pipeline().remove("6");
                ctx.pipeline().remove("7");
                for (ChannelHandler handler : toAdd) {
                    ctx.pipeline().addLast(handler);
                }
                onChannelEstablished(ctx.channel());
            } else {
                ctx.close();
            }
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            Socks5InitialRequest socks5InitialRequest = new DefaultSocks5InitialRequest(Socks5AuthMethod.NO_AUTH);
            ctx.writeAndFlush(socks5InitialRequest);
            ctx.fireChannelActive();
        }
    }
}

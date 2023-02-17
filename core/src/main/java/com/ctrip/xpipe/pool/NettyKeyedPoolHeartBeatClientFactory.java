package com.ctrip.xpipe.pool;

import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.netty.commands.NettyClientHandler;
import com.ctrip.xpipe.netty.commands.NettyKeyedPoolClientFactory;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;

public class NettyKeyedPoolHeartBeatClientFactory extends NettyKeyedPoolClientFactory {

    private static final int DEFAULT_READ_IDLE_SECONDS = 60;

    private final ChannelHandler idleHandler;

    public NettyKeyedPoolHeartBeatClientFactory(ChannelHandler idleHandler) {
        this.idleHandler = idleHandler;
    }

    @Override
    protected void initBootstrap() {
        b.group(eventLoopGroup).channel(NioSocketChannel.class)
                .option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(512))
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMilli)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new LoggingHandler());
                        p.addLast(new NettySimpleMessageHandler());
                        p.addLast(new NettyClientHandler());
                        p.addLast(new IdleStateHandler(DEFAULT_READ_IDLE_SECONDS, 0, 0));
                        p.addLast(idleHandler);
                    }
                });
    }
}

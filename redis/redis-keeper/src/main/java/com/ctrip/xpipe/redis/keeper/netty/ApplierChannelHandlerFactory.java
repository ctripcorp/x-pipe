package com.ctrip.xpipe.redis.keeper.netty;

import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.netty.commands.NettyClientHandler;
import com.ctrip.xpipe.pool.ChannelHandlerFactory;
import io.netty.channel.ChannelHandler;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.LinkedList;
import java.util.List;

public class ApplierChannelHandlerFactory implements ChannelHandlerFactory {

    private static final int DEFAULT_READ_IDLE_SECONDS = 60;

    @Override
    public List<ChannelHandler> createHandlers() {
        List<ChannelHandler> channelHandlers = new LinkedList<>();
        channelHandlers.add(new LoggingHandler());
        channelHandlers.add(new NettySimpleMessageHandler());
        channelHandlers.add(new NettyClientHandler());
        channelHandlers.add(new IdleStateHandler(DEFAULT_READ_IDLE_SECONDS, 0,0));
        channelHandlers.add(new NettyApplierIdleHandler());
        return channelHandlers;
    }
}

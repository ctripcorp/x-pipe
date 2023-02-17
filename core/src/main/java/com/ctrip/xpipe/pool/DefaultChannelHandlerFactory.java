package com.ctrip.xpipe.pool;

import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.netty.commands.NettyClientHandler;
import io.netty.channel.ChannelHandler;
import io.netty.handler.logging.LoggingHandler;

import java.util.LinkedList;
import java.util.List;

public class DefaultChannelHandlerFactory implements ChannelHandlerFactory {

    @Override
    public List<ChannelHandler> createHandlers() {
        List<ChannelHandler> channelHandlers = new LinkedList<>();
        channelHandlers.add(new LoggingHandler());
        channelHandlers.add(new NettySimpleMessageHandler());
        channelHandlers.add(new NettyClientHandler());
        return channelHandlers;
    }
}

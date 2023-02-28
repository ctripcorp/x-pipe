package com.ctrip.xpipe.pool;

import io.netty.channel.ChannelHandler;

import java.util.List;

public interface ChannelHandlerFactory {

    List<ChannelHandler> createHandlers();
}

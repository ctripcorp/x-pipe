package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.netty.commands.NettyKeyedPoolClientFactory;
import com.ctrip.xpipe.pool.ChannelHandlerFactory;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.ChannelOption;

/**
 * @author TB
 * <p>
 * 2025/11/13 13:58
 */
public class ApplierNettyPoolClientFactory extends NettyKeyedPoolClientFactory {

    private static final int MIN_SIZE = 512;

    private static final int INITIAL_SIZE = 2048;

    private static final int MAX_SIZE = 128 * 1024;

    public ApplierNettyPoolClientFactory(ChannelHandlerFactory channelHandlerFactory) {
        super(channelHandlerFactory);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
    }

    @Override
    protected void initBootstrap() {
        super.initBootstrap();
        b.option(ChannelOption.RCVBUF_ALLOCATOR,new AdaptiveRecvByteBufAllocator(MIN_SIZE,INITIAL_SIZE,MAX_SIZE));
    }
}

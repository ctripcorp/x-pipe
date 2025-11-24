package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.netty.commands.NettyKeyedPoolClientFactory;
import com.ctrip.xpipe.pool.ChannelHandlerFactory;
import io.netty.channel.ChannelOption;
import io.netty.channel.FixedRecvByteBufAllocator;

/**
 * @author TB
 * <p>
 * 2025/11/13 13:58
 */
public class ApplierNettyPoolClientFactory extends NettyKeyedPoolClientFactory {

    private static final int DEFAULT_BUFFER_SIZE = 512;

    private int bufferSize = DEFAULT_BUFFER_SIZE;

    public ApplierNettyPoolClientFactory(ChannelHandlerFactory channelHandlerFactory) {
        super(channelHandlerFactory);
    }

    public ApplierNettyPoolClientFactory(ChannelHandlerFactory channelHandlerFactory, int bufferSize) {
        super(channelHandlerFactory);
        this.bufferSize = bufferSize;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
    }

    @Override
    protected void initBootstrap() {
        super.initBootstrap();
        b.option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(bufferSize));
    }
}

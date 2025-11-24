package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.ChannelHandlerFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.commons.pool2.PooledObject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;

/**
 * @author TB
 * <p>
 * 2025/11/13 14:05
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class ApplierNettyPoolClientFactoryTest {
    @Mock
    private ChannelHandlerFactory channelHandlerFactory;
    @Test
    public void testBootstrapRecvBufAllocator() throws Exception {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(new NioEventLoopGroup(1), new NioEventLoopGroup(1))
                .channel(NioServerSocketChannel.class)
                .childHandler(new SimpleChannelInboundHandler<ByteBuf>() {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                        // 回显接收到的数据
                        ctx.writeAndFlush(msg.retain());
                    }
                });

        ChannelFuture serverFuture = serverBootstrap.bind(0);
        Assert.assertTrue(serverFuture.sync().isSuccess());

        Channel serverChannel = serverFuture.channel();
        int port = ((InetSocketAddress) serverChannel.localAddress()).getPort();


        ApplierNettyPoolClientFactory applierNettyPoolClientFactory = new ApplierNettyPoolClientFactory(channelHandlerFactory);
        applierNettyPoolClientFactory.doStart();
        PooledObject<NettyClient> pooledObject = applierNettyPoolClientFactory.makeObject(new DefaultEndPoint("localhost",port));
        RecvByteBufAllocator recvByteBufAllocator = pooledObject.getObject().channel().config().getRecvByteBufAllocator();
        Assert.assertTrue(recvByteBufAllocator instanceof AdaptiveRecvByteBufAllocator);
    }
}

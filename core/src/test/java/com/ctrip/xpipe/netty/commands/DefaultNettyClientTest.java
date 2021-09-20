package com.ctrip.xpipe.netty.commands;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultNettyClientTest extends AbstractTest {

    private Server server;

    private DefaultNettyClient nettyClient;

    private Bootstrap b = new Bootstrap();

    private ByteBufReceiver receiver = Mockito.mock(ByteBufReceiver.class);

    @Before
    public void setupDefaultNettyClientTest() throws Exception {
        server = startEchoServer();
        initBootstrap();
        Channel channel = b.connect("127.0.0.1", server.getPort()).channel();
        waitConditionUntilTimeOut(channel::isActive, 2000);
        nettyClient = new DefaultNettyClient(channel);
        channel.attr(NettyClientHandler.KEY_CLIENT).set(nettyClient);
    }

    @After
    public void afterDefaultNettyClientTest() throws Exception {
        if (null != server) server.stop();
    }

    @Test
    public void testSendRequest() throws Exception {
        String msg = "test\r\n";
        AtomicBoolean callReceive = new AtomicBoolean(false);

        Mockito.doAnswer(invocation -> {
            callReceive.set(true);
            ByteBuf byteBuf = invocation.getArgument(1, ByteBuf.class);
            Assert.assertEquals(msg, byteBuf.toString());
            return ByteBufReceiver.RECEIVER_RESULT.SUCCESS;
        }).when(receiver).receive(Mockito.any(), Mockito.any());
        nettyClient.sendRequest(Unpooled.copiedBuffer(msg.getBytes()), receiver);

        waitConditionUntilTimeOut(callReceive::get, 2000);
    }

    @Test
    public void testSendOnChannelClose() {
        String msg = "test\r\n";
        nettyClient.channel().close();
        nettyClient.sendRequest(Unpooled.copiedBuffer(msg.getBytes()), receiver);

        sleep(1000);
        Mockito.verify(receiver, Mockito.never()).receive(Mockito.any(), Mockito.any());
        Mockito.verify(receiver, Mockito.times(1)).clientClosed(Mockito.any(DefaultNettyClient.class), Mockito.any(Throwable.class));
    }

    private void initBootstrap() {
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1, XpipeThreadFactory.create("NettyKeyedPoolClientFactory"));

        b.group(eventLoopGroup).channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new LoggingHandler());
                        p.addLast(new NettySimpleMessageHandler());
                        p.addLast(new NettyClientHandler());
                    }
                });
    }

}

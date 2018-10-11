package com.ctrip.xpipe.netty.commands;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.pool.BorrowObjectException;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPoolTest;
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Lists;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.internal.PendingWrite;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Sep 28, 2018
 */
public class AsyncNettyClientTest extends AbstractTest {

    private XpipeNettyClientKeyedObjectPool pool;

    private Bootstrap b = new Bootstrap();

    private Server server;

    private String result = "";

    @Before
    public void beforeXpipeNettyClientKeyedObjectPoolTest() throws Exception{
        doStart();
        server = startEchoServer();
    }

//    @Test
    public void testFutureSend() {
        ChannelFuture future = b.connect("localhost", server.getPort());
        int N = 1000;
        for(int i = 0; i < N; i++) {
            if (future.isSuccess()) {
                logger.info("[success]");
                future.channel().writeAndFlush("success" + i + "\r\n");
            } else {
                int finalI = i;
                future.addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        logger.info("[listener]");
                        future.channel().writeAndFlush("listener " + finalI + "\r\n");
                    }
                });
            }
            sleep(1);
        }
        sleep(1000 * 10);
    }

//    @Test
    public void testSendActive() {
        ChannelFuture future = b.connect("10.5.111.145", 6379);
        int N = 1000;
        for(int i = 0; i < N; i++) {
            if (future.channel().isActive()) {
                logger.info("[success] success {}", i);
            } else {
                int finalI = i;
                future.addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        logger.info("[listener] listener {}", finalI);
                    }
                });
            }
            sleep(1);
        }
        sleep(1000 * 10);
    }

    @Test
    public void testSendRequest() throws TimeoutException {
        AsyncNettyClient client = new AsyncNettyClient(b.connect("localhost", server.getPort()),
                new DefaultEndPoint("localhost", server.getPort()));
        client.channel().attr(NettyClientHandler.KEY_CLIENT).set(client);

        StringBuffer sb = new StringBuffer();

        int N = 100;
        for(int i = 0; i < N; i++) {
            String message = i + "\r\n";
            client.sendRequest(Unpooled.copiedBuffer(message.getBytes()), new ByteBufReceiver() {
                @Override
                public RECEIVER_RESULT receive(Channel channel, ByteBuf byteBuf) {
                    sb.append(byteBuf.toString(Charset.defaultCharset()));
                    return RECEIVER_RESULT.SUCCESS;
                }

                @Override
                public void clientClosed(NettyClient nettyClient) {

                }
            });
        }
//        waitConditionUntilTimeOut(()->client.channel().isActive(), 1000);
        sleep(5 * 1000);
        String str = sb.toString();
        String[] recipents = StringUtil.splitByLineRemoveEmpty(str);
        String[] expected = Arrays.copyOf(recipents, recipents.length);
        for(String receive : recipents) {
            logger.info("[receive] {}", receive);
        }
    }

    protected void doStart() throws Exception {

        EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1, XpipeThreadFactory.create("NettyKeyedPoolClientFactory"));

        b.group(eventLoopGroup).channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
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

    class ReceiveHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if(msg instanceof ByteBuf) {
                String local = ((ByteBuf) msg).toString(Charset.defaultCharset());
                logger.info("[receive] {}", local);
                result += local;
            }
            super.channelRead(ctx, msg);
        }
    }
}
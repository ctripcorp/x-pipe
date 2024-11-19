package com.ctrip.xpipe.redis.core.client;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.netty.commands.AsyncNettyClient;
import com.ctrip.xpipe.netty.commands.ByteBufReceiver;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.NettyClientHandler;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import java.util.concurrent.TimeoutException;

/**
 * @author chen.zhu
 * <p>
 * Sep 28, 2018
 */
public class AsyncNettyClientTest extends AbstractTest {

    private XpipeNettyClientKeyedObjectPool pool;

    protected Bootstrap b = new Bootstrap();

    protected Server server;

    protected String result = "";

    @Before
    public void beforeXpipeNettyClientKeyedObjectPoolTest() throws Exception{
        doStart();
        server = startEchoServer();
    }

    @Ignore
    @Test
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

    @Ignore
    @Test
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

        StringBuilder expected = new StringBuilder();

        int N = 100;
        runTheTest(client, sb, expected, N);
        waitConditionUntilTimeOut(()->client.channel().isActive(), 1000);
        sleep(1000);
        String str = sb.toString();
        Assert.assertEquals(str, expected.toString());
    }

    @Test
    public void testFutureClosed() {
        AsyncNettyClient client = new AsyncNettyClient(b.connect("localhost", server.getPort()),
                new DefaultEndPoint("localhost", server.getPort()));
        client.channel().attr(NettyClientHandler.KEY_CLIENT).set(client);

        StringBuffer sb = new StringBuffer();

        StringBuilder expected = new StringBuilder();

        int N = 100;
        new Thread(new Runnable() {
            @Override
            public void run() {
                runTheTest(client, sb, expected, N);
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    server.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
//        waitConditionUntilTimeOut(()->client.channel().isActive(), 1000);
        sleep(2 * 1000);
    }

    protected void runTheTest(AsyncNettyClient client, StringBuffer sb, StringBuilder expected, int n) {
        this.runTheTest(client, sb, expected, n, null);
    }

    protected void runTheTest(AsyncNettyClient client, StringBuffer sb, StringBuilder expected, int n, String prefix) {
        for(int i = 0; i < n; i++) {
            String message = "+" + i + "\r\n";
            client.sendRequest(Unpooled.copiedBuffer(message.getBytes()), new ByteBufReceiver() {

                private RedisClientProtocol<String> parser = new SimpleStringParser();
                @Override
                public RECEIVER_RESULT receive(Channel channel, ByteBuf byteBuf) {
                    RedisClientProtocol<String> clientProtocol = parser.read(byteBuf);
                    if(clientProtocol != null) {
                        sb.append(clientProtocol.getPayload());
                        return RECEIVER_RESULT.SUCCESS;
                    }
                    return RECEIVER_RESULT.CONTINUE;
                }

                @Override
                public void clientClosed(NettyClient nettyClient) {

                }

                @Override
                public void clientClosed(NettyClient nettyClient, Throwable th) {

                }
            });
            if (prefix != null) {
                expected.append(prefix).append("+");
            }
            expected.append(i);
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

}
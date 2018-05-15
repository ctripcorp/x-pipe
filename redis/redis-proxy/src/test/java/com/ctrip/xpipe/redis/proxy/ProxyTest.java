package com.ctrip.xpipe.redis.proxy;


import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.utils.ChannelUtil;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;


/**
 * @author chen.zhu
 * <p>
 * May 08, 2018
 */
public class ProxyTest extends AbstractRedisProxyServerTest {

    @Test
    public void startServer1() throws Exception {
        startServer(8009, new Function<String, String>() {
            @Override
            public String apply(String s) {
                System.out.println(s);
                if(s.toLowerCase().contains("proxy")) {
                    return null;
                }
                return s;
            }
        });
//        ServerSocket socket = new ServerSocket();
//        socket.bind(new InetSocketAddress("127.0.0.1", 8009));
//        Socket socket1 = socket.accept();
//        while(socket1.getInputStream() != null) {
//            byte[] bytes = new byte[2048];
//            socket1.getInputStream().read(bytes);
//            System.out.println(new String(bytes, Charset.defaultCharset()));
//        }
        Thread.sleep(1000 * 60 * 60);
    }

    @Test
    public void testSendFrequently() throws InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        int N = 10000;
        serverBootstrap().childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(new StringEncoder());
                ch.pipeline().addLast(new StringDecoder());
                ch.pipeline().addLast(new ChannelInboundHandlerAdapter(){
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                        Assert.assertTrue(msg instanceof String);
                        String message = (String) msg;
                        Assert.assertTrue(message.toLowerCase().contains("ok") || message.toLowerCase().contains("proxy"));
                        if (message.toLowerCase().contains("ok")) {
                            String[] strs = message.split("\r\n");
                            for(String str : strs) {
                                if(str.toLowerCase().contains("ok")) {
                                    counter.getAndIncrement();
                                }
                            }
                        }
                        super.channelRead(ctx, msg);
                    }

                    @Override
                    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
//                        Assert.assertEquals(N, counter.get());
                        logger.info("[complete] N: {} counter: {}, N == counter: {}", N, counter.get(), N == counter.get());
                        System.out.println("Complete" + counter.get());
                        super.channelInactive(ctx);
                    }
                });
            }
        }).bind(8009).sync();
        ChannelFuture future = clientBootstrap().connect("127.0.0.1", 8992)
                .addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        future.channel().writeAndFlush(new SimpleStringParser("Proxy Route proxy://127.0.0.1:8009").format());
                        Thread.sleep(1);
                        for(int i = 0; i < N; i++) {
                            future.channel().writeAndFlush(new SimpleStringParser("OK").format());
                        }
                    }
                });
        Thread.sleep(1000 * 10);
        logger.info("[complete] N: {} counter: {}, N == counter: {}", N, counter.get(), N == counter.get());
    }

    @Test
    public void testCloseOnBothSide() throws Exception {
        serverBootstrap().childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(new StringEncoder());
                ch.pipeline().addLast(new StringDecoder());
                ch.pipeline().addLast(new ChannelInboundHandlerAdapter(){
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                        logger.info("[channelRead] msg: {}", msg);
                        super.channelRead(ctx, msg);
                    }

                    @Override
                    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
                        logger.info("[channelUnregistered] {}", ChannelUtil.getDesc(ctx.channel()));
                        super.channelUnregistered(ctx);
                    }

                    @Override
                    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                        logger.info("[channelInactive] {}", ChannelUtil.getDesc(ctx.channel()));
                        super.channelInactive(ctx);
                    }
                });
            }
        }).bind(8009).sync();
        ChannelFuture future = clientBootstrap().connect("127.0.0.1", 8992)
                .addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        future.channel().writeAndFlush(new SimpleStringParser("Proxy Route proxy://127.0.0.1:8009").format());
                    }
                });
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                future.channel().close().sync();
            }
        });

        Thread.sleep(1000 * 3);
    }

    @Test
    public void testCloseOnBothSide2() throws Exception {
        serverBootstrap().childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(new StringEncoder());
                ch.pipeline().addLast(new StringDecoder());
                ch.pipeline().addLast(new ChannelInboundHandlerAdapter(){
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                        logger.info("[channelRead] msg: {}", msg);
                        ctx.channel().close();
                        super.channelRead(ctx, msg);
                    }

                    @Override
                    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
                        logger.info("[channelUnregistered] {}", ChannelUtil.getDesc(ctx.channel()));
                        super.channelUnregistered(ctx);
                    }

                    @Override
                    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                        logger.info("[channelInactive] {}", ChannelUtil.getDesc(ctx.channel()));
                        super.channelInactive(ctx);
                    }
                });
            }
        }).bind(8009).sync();
        ChannelFuture future = clientBootstrap().connect("127.0.0.1", 8992)
                .addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        future.channel().writeAndFlush(new SimpleStringParser("Proxy Route proxy://127.0.0.1:8009").format());
                    }
                });

        future.channel().closeFuture().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                logger.info("[close] close future");
            }
        });

        Thread.sleep(1000 * 3);
    }
}
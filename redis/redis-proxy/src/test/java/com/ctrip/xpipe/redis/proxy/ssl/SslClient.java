package com.ctrip.xpipe.redis.proxy.ssl;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * @author chen.zhu
 * <p>
 * May 08, 2018
 */
public class SslClient {
    static final String HOST = System.getProperty("host", "127.0.0.1");
    static final int PORT = Integer.parseInt(System.getProperty("port", "9090"));

    public static void main(String[] args) throws Exception {
//        // Configure SSL.
//        final SslContext sslCtx = SslContextBuilder.forClient()
//                .trustManager(InsecureTrustManagerFactory.INSTANCE).build();

        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new SecureChatClientInitializer());

            ChannelFuture future = b.connect(HOST, PORT);



//            future.sync().addListener(new ChannelFutureListener() {
//                @Override
//                public void operationComplete(ChannelFuture future) throws Exception {
//                    if(future.isSuccess()) {
//                        SimpleStringParser parser = new SimpleStringParser("Proxy Route proxy://127.0.0.1:9090");
//                        future.channel().writeAndFlush(parser.format());
//                    } else {
//                        System.out.println("Future fails: " + future.cause());
//                    }
//                }
//            });
            future.channel().closeFuture().sync();
        } finally {
            // The connection is closed automatically on shutdown.
            group.shutdownGracefully();
        }
    }
}

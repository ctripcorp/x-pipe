package com.ctrip.xpipe.redis.proxy.ssl;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.security.cert.CertificateException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author chen.zhu
 * <p>
 * May 08, 2018
 */
public class SslServer {

    static final int LOCAL_PORT = Integer.parseInt(System.getProperty("localPort", "9090"));

    static final Logger logger = LoggerFactory.getLogger(SslServer.class);

    public static void main(String[] args) throws CertificateException, SSLException, InterruptedException {

        ChannelFuture future = null;

        ExecutorService executorService = Executors.newSingleThreadExecutor();

//        // Normal Client
//        EventLoopGroup group = new NioEventLoopGroup();
//        try {
//            Bootstrap b = new Bootstrap();
//            b.group(group)
//                    .channel(NioSocketChannel.class)
//                    .handler(new ChannelInitializer<Channel>() {
//                        @Override
//                        protected void initChannel(Channel ch) throws Exception {
//                            ChannelPipeline pipeLine = ch.pipeline();
//                            pipeLine.addLast(new LoggingHandler());
//                        }
//                    });
//
//            // Start the connection attempt.
//            future = b.connect("127.0.0.1", NormalServer.LOCAL_PORT);
//
//
//        } finally {
//            // The connection is closed automatically on shutdown.
//            group.shutdownGracefully();
//        }

        // SSL Server

        SelfSignedCertificate ssc = new SelfSignedCertificate();
        SslContext sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            ChannelFuture finalFuture = future;
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new SecureChatServerInitializer());
            b.bind(9090).sync().channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }



    }
}

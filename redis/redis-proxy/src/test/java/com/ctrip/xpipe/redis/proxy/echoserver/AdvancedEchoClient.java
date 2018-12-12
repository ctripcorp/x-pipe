package com.ctrip.xpipe.redis.proxy.echoserver;

import com.ctrip.xpipe.redis.proxy.echoserver.handler.ReadOnlyEchoClientHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author chen.zhu
 * <p>
 * Jul 09, 2018
 */
public class AdvancedEchoClient {
    private static final Logger logger = LoggerFactory.getLogger(AdvancedEchoClient.class);
    private static final int MEGA_BYTE = 1024 * 1024;
    private static final int KILO_BYTE = 1024;

    private static int MESSAGE_SIZE = 1024;

    private static final String CRLF = "\r\n";

    private static String DELIMIETER_STR = "aA";

    private static ByteBuf DELIMITER = Unpooled.copiedBuffer(DELIMIETER_STR.getBytes());

    private String host;

    private int port;

    private String protocol;

    private int speed;

    private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());

    public AdvancedEchoClient(String host, int port, int speed) {
        this.host = host;
        this.port = port;
        this.speed = speed;
    }

    public AdvancedEchoClient(String host, int port, String protocol, int speed) {
        this.host = host;
        this.port = port;
        this.protocol = protocol;
        this.speed = speed;
    }

    public ChannelFuture startServer() {
        String message = new LengthBasedGenerator(MESSAGE_SIZE, new char[]{'a', 'A'}).message();
        Bootstrap bootstrap = bootstrap(message, MESSAGE_SIZE + 1024);
        System.out.println("host: " + host + ", port: " + port);
        ChannelFuture future = bootstrap.connect(host, port).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if(protocol != null && !protocol.isEmpty()) {
                    future.channel().writeAndFlush(protocol + CRLF);
                    logger.info("[connect] protocol: {}", protocol);
                    System.out.println("[connect] protocol: " + protocol);
                }
            }
        });

        AtomicLong counter = new AtomicLong(0);

        long microPerSecond = TimeUnit.SECONDS.toMicros(1);
        long numberOfMessagePerSec = speed / MESSAGE_SIZE;
        long microsPerMessage = microPerSecond/numberOfMessagePerSec;
        final Channel channel = future.channel();
        scheduled.scheduleAtFixedRate(()->{
            channel.writeAndFlush(message + DELIMIETER_STR);
            counter.getAndAdd(message.getBytes().length);

        }, 0, microsPerMessage, TimeUnit.MICROSECONDS);

        scheduled.scheduleAtFixedRate(()->{
            long now = counter.getAndSet(0);
            if(now >= MEGA_BYTE) {
                logger.info("[sendout] {} MB", now / MEGA_BYTE);
            } else if(now >= KILO_BYTE) {
                logger.info("[sendout] {} KB", now / KILO_BYTE);
            } else {
                logger.info("[sendout] {}", now);
            }
        }, 5, 5, TimeUnit.SECONDS);
        logger.info("[connect]send out: {}", message + DELIMIETER_STR);
        return future;
    }

    private Bootstrap bootstrap(String message, int maxSize) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(new NioEventLoopGroup(1))
                .option(ChannelOption.TCP_NODELAY, true)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new DelimiterBasedFrameDecoder(maxSize, DELIMITER));
                        p.addLast(new StringEncoder());
                        p.addLast(new StringDecoder());
                        p.addLast(new ReadOnlyEchoClientHandler(message));
                    }
                });
        return bootstrap;
    }
}

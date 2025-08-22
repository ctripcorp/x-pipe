package com.ctrip.xpipe.netty;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * @author wenchao.meng
 *         <p>
 *         Jan 26, 2018
 */
public class TcpPortCheckCommand extends AbstractCommand<Boolean> {

    public static int CHECK_TIMEOUT_MILLI = 10000;
    private String host;
    private int port;
    private static Logger logger = LoggerFactory.getLogger(TcpPortCheckCommand.class);
    private static LoggingHandler loggingHandler = new LoggingHandler(LogLevel.DEBUG);
    private static NioEventLoopGroup nioEventLoopGroup = new NioEventLoopGroup(1, XpipeThreadFactory.create("tcp-port-check", true));
    private Bootstrap b = new Bootstrap();

    public TcpPortCheckCommand(String host, int port) {
        this(host, port, CHECK_TIMEOUT_MILLI);
    }

    public TcpPortCheckCommand(String host, int port, int connectTimeoutMilli) {
        this.host = host;
        this.port = port;
        this.b.group(nioEventLoopGroup);
        this.b.option(ChannelOption.ALLOCATOR, UnpooledByteBufAllocator.DEFAULT);
        this.b.channel(NioSocketChannel.class);
        this.b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMilli);

        this.b.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(loggingHandler);
                pipeline.addLast(new ChannelDuplexHandler() {
                    @Override
                    public void channelActive(ChannelHandlerContext ctx) throws Exception {
                        ctx.channel().close();
                    }
                });
            }
        });
    }

    @Override
    public String getName() {
        return "[TcpPortCheckCommand]";
    }

    @Override
    protected void doExecute() throws Exception {

        ChannelFuture connect = b.connect(new InetSocketAddress(host, port));
        connect.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {

                if (future.isSuccess()) {
                    future().setSuccess(true);
                } else {
                    logger.warn("[doExecute]", future.cause());
                    future().setFailure(future.cause());
                }
            }
        });

    }

    @Override
    protected void doReset() {

    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}

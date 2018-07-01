package com.ctrip.xpipe.redis.proxy.ssl;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.ssl.SslHandler;

/**
 * @author chen.zhu
 * <p>
 * May 08, 2018
 */
public class ServerHandlersInit extends ChannelInitializer<SocketChannel> {

    public ServerHandlersInit() {

    }

    protected void initChannel(SocketChannel socketChannel) throws Exception {

        SslHandler sslHandler = SSLHandlerProvider.getSSLHandler();

        ChannelPipeline pipeline = socketChannel.pipeline();
        pipeline.addLast(sslHandler);
        pipeline.addLast(new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter()));
        pipeline.addLast(new StringDecoder());
        pipeline.addLast(new StringEncoder());

        // and then business logic.
        pipeline.addLast(new SecureChatServerHandler());
    }
}

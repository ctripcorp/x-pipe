package com.ctrip.xpipe.redis.proxy.echoserver.handler;

import com.dianping.cat.Cat;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chen.zhu
 * <p>
 * Jun 12, 2018
 */
public class ReadOnlyEchoClientHandler extends ChannelInboundHandlerAdapter {

    private Logger logger = LoggerFactory.getLogger(ReadOnlyEchoClientHandler.class);

    private String message;

    public ReadOnlyEchoClientHandler(String message) {
        this.message = message;
    }


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if(!(msg instanceof String)) {
            Cat.logEvent("UNEXPECTED.TYPE", msg.getClass().getName());
        }
        if (!message.equals(msg)) {
            Cat.logEvent("Not.Equals", (String) msg);
            logger.info("[not equals][receive]{}", msg);
            logger.info("[not equals][expect]{}", message);
            System.out.println("[not equal]");
        }
        logger.debug("SEND.OUT: {}", message);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }
}

package com.ctrip.xpipe.redis.keeper.netty;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyApplierIdleHandler extends ChannelDuplexHandler {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if(evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            switch (event.state()) {
                case READER_IDLE:
                    EventMonitor.DEFAULT.logAlertEvent(String.format("[NettyApplierIdleHandler] xsync channel %s idle too long", ctx.channel().toString()));
                    logger.warn("[NettyApplierIdleHandler] channel {} idle too long, force close", ctx.channel().toString());
                    ctx.close();
            }
        }

    }
}

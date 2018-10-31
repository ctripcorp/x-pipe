package com.ctrip.xpipe.redis.core.proxy.protocols;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyCommand;
import com.ctrip.xpipe.api.proxy.ProxyRequestResponseProtocol;
import com.ctrip.xpipe.redis.core.proxy.ProxyReqResProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.command.ProxyPingCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chen.zhu
 * <p>
 * Oct 24, 2018
 */
public class DefaultProxyReqResProtocol extends AbstractProxyProtocol<ProxyReqResProtocolParser>
        implements ProxyRequestResponseProtocol {

    private static final Logger logger = LoggerFactory.getLogger(DefaultProxyReqResProtocol.class);

    public DefaultProxyReqResProtocol(ProxyReqResProtocolParser parser) {
        super(parser);
    }

    @Override
    public void response(Channel channel) {
        parser.getAsyncResponse(channel).addListener(new CommandFutureListener<ByteBuf>() {
            @Override
            public void operationComplete(CommandFuture<ByteBuf> future) throws Exception {
                if(future.isSuccess()) {
                    channel.writeAndFlush(future.get());
                } else {
                    logger.error("[response]", future.cause());
                }
            }
        });
    }
}

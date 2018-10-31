package com.ctrip.xpipe.redis.core.proxy.parser;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.proxy.ProxyRequestResponseProtocol;
import com.ctrip.xpipe.redis.core.proxy.ProxyReqResProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.protocols.DefaultProxyReqResProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 24, 2018
 */
public class DefaultProxyReqResProtocolParser extends AbstractProxyProtocolParser<ProxyRequestResponseProtocol>
        implements ProxyReqResProtocolParser {

    @Override
    protected ProxyRequestResponseProtocol newProxyProtocol(String protocol) {
        return new DefaultProxyReqResProtocol(this);
    }

    @Override
    protected void validate(ProxyRequestResponseProtocol proxyProtocol, List<ProxyOptionParser> parsers) {
        if(parsers.size() > 1) {
            throw new IllegalArgumentException("ProxyRequestResponseProtocol should be one request");
        }
    }

}

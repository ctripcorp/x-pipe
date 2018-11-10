package com.ctrip.xpipe.redis.core.proxy.protocols;

import com.ctrip.xpipe.api.proxy.ProxyProtocol;
import com.ctrip.xpipe.api.proxy.ProxyRequestResponseProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.core.proxy.ProxyReqResProtocolParser;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chen.zhu
 * <p>
 * Oct 24, 2018
 */
public class DefaultProxyReqResProtocol implements ProxyRequestResponseProtocol {

    private static final Logger logger = LoggerFactory.getLogger(DefaultProxyReqResProtocol.class);

    private ProxyReqResProtocolParser parser;

    private String content;

    public DefaultProxyReqResProtocol(ProxyReqResProtocolParser parser) {
        this.parser = parser;
    }

    public DefaultProxyReqResProtocol(String content) {
        this.content = content;
    }

    @Override
    public String getContent() {
        return content == null ? parser.getContent() : content;
    }

    @Override
    public ByteBuf output() {
        return new SimpleStringParser(String.format("%s %s", ProxyProtocol.KEY_WORD, getContent())).format();
    }
}

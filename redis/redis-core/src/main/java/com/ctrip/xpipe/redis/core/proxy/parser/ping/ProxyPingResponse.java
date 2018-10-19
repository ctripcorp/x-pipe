package com.ctrip.xpipe.redis.core.proxy.parser.ping;

import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyProtocolResponse;
import io.netty.buffer.ByteBuf;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2018
 */
public class ProxyPingResponse extends AbstractProxyProtocolResponse<String> {

    private static final String PONG_TEMPLATE = "PONG %s:%d";

    private SimpleStringParser simpleStringParser;

    public ProxyPingResponse(String host, int port) {
        payload = String.format(PONG_TEMPLATE, host, port);
        simpleStringParser = new SimpleStringParser(payload);
    }

    public ProxyPingResponse() {
        simpleStringParser = new SimpleStringParser();
    }

    @Override
    public ByteBuf format() {
        if(simpleStringParser == null) {
            throw new IllegalStateException("SimpleStringParser Null Pointer");
        }
        return simpleStringParser.format();
    }

    @Override
    public String getPayload() {
        return simpleStringParser.getPayload();
    }

    @Override
    public boolean tryRead(ByteBuf buffer) {
        RedisClientProtocol parser = simpleStringParser.read(buffer);
        if(parser == null) {
            return false;
        }
        simpleStringParser = (SimpleStringParser) parser;
        return true;
    }
}

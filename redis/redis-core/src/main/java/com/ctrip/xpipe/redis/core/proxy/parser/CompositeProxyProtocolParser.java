package com.ctrip.xpipe.redis.core.proxy.parser;

import com.ctrip.xpipe.api.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.ProxyProtocolParser;
import io.netty.buffer.ByteBuf;

import static com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser.LINE_SPLITTER;

/**
 * @author chen.zhu
 * <p>
 * Nov 01, 2018
 */
public class CompositeProxyProtocolParser implements ProxyProtocolParser {

    private ProxyProtocolParser parser;

    private SimpleStringParser simpleStringParser = new SimpleStringParser();

    @Override
    public ByteBuf format() {
        throw new UnsupportedOperationException("CompositeProxyProtocolParser not support format");
    }

    @Override
    public <T extends ProxyProtocol> T read(final String protocol) {
        String proto = removeKeyWord(protocol);
        String[] allOption = proto.split(LINE_SPLITTER);
        for(String option : allOption) {
            boolean responsible = PROXY_OPTION.parse(option.trim()).hasResponse();
            if(responsible) {
                parser = new DefaultProxyReqResProtocolParser();
            } else {
                parser = new DefaultProxyConnectProtocolParser();
            }
        }
        if(parser != null) {
            return parser.read(protocol);
        }
        return null;
    }

    @Override
    public <T extends ProxyProtocol> T read(ByteBuf byteBuf) {
        RedisClientProtocol<String> redisClientProtocol = simpleStringParser.read(byteBuf);
        if(redisClientProtocol == null) {
            return null;
        } else {
            resetSimpleStringParser();
        }
        return read(redisClientProtocol.getPayload());
    }

    private void resetSimpleStringParser() {
        simpleStringParser = new SimpleStringParser();
    }

    @Override
    public ProxyOptionParser getProxyOptionParser(PROXY_OPTION option) {
        return option.getProxyOptionParser();
    }

    private String removeKeyWord(String protocol) {
        return protocol.substring(ProxyProtocol.KEY_WORD.length());
    }
}

package com.ctrip.xpipe.redis.core.proxy;

import com.ctrip.xpipe.redis.core.exception.ProxyProtocolException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.core.proxy.parser.ProxyOptionParser;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;

import java.util.List;

import static com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser.LINE_SPLITTER;
import static com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser.WHITE_SPACE;

/**
 * @author chen.zhu
 * <p>
 * May 04, 2018
 */
public class DefaultProxyProtocolParser implements ProxyProtocolParser {

    private SimpleStringParser simpleStringParser = new SimpleStringParser();

    private List<ProxyOptionParser> parsers = Lists.newArrayList();

    public void addProxyParser(ProxyOptionParser parser) {
        parsers.add(parser);
    }

    @Override
    public ProxyOptionParser getProxyOptionParser(PROXY_OPTION proxyOption) {
        for(ProxyOptionParser parser : parsers) {
            if(parser.option() == proxyOption) {
                return parser;
            }
        }
        ProxyOptionParser parser = proxyOption.getProxyOptionParser();
        addProxyParser(parser);
        return parser;
    }

    @Override
    public ByteBuf format() {
        StringBuilder proxyProtocol = new StringBuilder(ProxyProtocol.KEY_WORD).append(WHITE_SPACE);
        for(ProxyOptionParser parser : parsers) {
            proxyProtocol.append(parser.getPayload()).append(";");
        }
        return new SimpleStringParser(proxyProtocol.toString()).format();
    }



    @Override
    public ProxyProtocol read(String protocol) {
        if(!protocol.toLowerCase().startsWith(ProxyProtocol.KEY_WORD.toLowerCase())) {
            throw new ProxyProtocolException("proxy protocol format error: " + protocol);
        }
        ProxyProtocol proxyProtocol = new DefaultProxyProtocol(this);
        proxyProtocol.setContent(protocol);

        protocol = removeKeyWord(protocol);
        String[] allOption = protocol.split(LINE_SPLITTER);
        for(String option : allOption) {
            addProxyParser(PROXY_OPTION.parse(option.trim()));
        }
        return proxyProtocol;
    }

    @Override
    public ProxyProtocol read(ByteBuf byteBuf) {
        RedisClientProtocol<String> redisClientProtocol = simpleStringParser.read(byteBuf);
        if(redisClientProtocol == null) {
            return null;
        }
        return read(redisClientProtocol.getPayload());
    }

    private String removeKeyWord(String protocol) {
        return protocol.substring(ProxyProtocol.KEY_WORD.length());
    }
}

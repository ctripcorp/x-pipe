package com.ctrip.xpipe.redis.core.proxy.parser;

import com.ctrip.xpipe.api.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.core.exception.ProxyProtocolException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.ProxyProtocolParser;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;

import java.util.List;

import static com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser.LINE_SPLITTER;
import static com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser.WHITE_SPACE;

/**
 * @author chen.zhu
 * <p>
 * Oct 24, 2018
 */
public abstract class AbstractProxyProtocolParser<V extends ProxyProtocol> implements ProxyProtocolParser {

    private SimpleStringParser simpleStringParser = new SimpleStringParser();

    private List<ProxyOptionParser> parsers = Lists.newArrayList();

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
            proxyProtocol.append(parser.output()).append(";");
        }
        return new SimpleStringParser(proxyProtocol.toString()).format();
    }

    @Override
    public V read(String protocol) {
        if(!protocol.toLowerCase().startsWith(ProxyProtocol.KEY_WORD.toLowerCase())) {
            throw new ProxyProtocolException("proxy protocol format error: " + protocol);
        }
        V proxyProtocol = newProxyProtocol(protocol);

        protocol = removeKeyWord(protocol);
        String[] allOption = protocol.split(LINE_SPLITTER);
        for(String option : allOption) {
            addProxyParser(PROXY_OPTION.parse(option.trim()));
        }
        validate(proxyProtocol, getParsers());
        return proxyProtocol;
    }

    @Override
    public V read(ByteBuf byteBuf) {
        RedisClientProtocol<String> redisClientProtocol = simpleStringParser.read(byteBuf);
        if(redisClientProtocol == null) {
            return null;
        }
        return read(redisClientProtocol.getPayload());
    }


    private String removeKeyWord(String protocol) {
        return protocol.substring(ProxyProtocol.KEY_WORD.length());
    }

    private void addProxyParser(ProxyOptionParser parser) {
        parsers.add(parser);
    }

    private List<ProxyOptionParser> getParsers() {
        return parsers;
    }

    protected abstract V newProxyProtocol(String protocol);

    protected abstract void validate(V proxyProtocol, List<ProxyOptionParser> parsers);
}

package com.ctrip.xpipe.redis.core.proxy.parser;

import com.ctrip.xpipe.api.proxy.ProxyProtocol;
import com.ctrip.xpipe.api.proxy.ProxyRequestResponseProtocol;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.core.proxy.ProxyReqResProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.protocols.DefaultProxyReqResProtocol;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 24, 2018
 */
public class DefaultProxyReqResProtocolParser extends AbstractProxyProtocolParser<ProxyRequestResponseProtocol> implements ProxyReqResProtocolParser {

    private String content;

    private ProxyOptionParser optionParser;

    @Override
    protected ProxyRequestResponseProtocol newProxyProtocol(String protocol) {
        optionParser = getParsers().get(0);
        if(protocol.contains(AbstractProxyOptionParser.LINE_SPLITTER)) {
            protocol = StringUtil.splitRemoveEmpty(AbstractProxyOptionParser.LINE_SPLITTER, protocol)[0];
        }
        this.content = removeKeyWord(protocol);
        return new DefaultProxyReqResProtocol(content);
    }

    @Override
    public String getContent() {
        return content;
    }

    @Override
    public ByteBuf format() {
        return new SimpleStringParser(String.format("%s %s", ProxyProtocol.KEY_WORD, optionParser.output())).format();
    }

    @Override
    protected void validate(List<ProxyOptionParser> parsers) {
        if(parsers.size() != 1) {
            throw new XpipeRuntimeException("Proxy ReqResp Protocol only send one option");
        }
    }
}

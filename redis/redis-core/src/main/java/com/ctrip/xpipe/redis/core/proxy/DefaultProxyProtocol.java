package com.ctrip.xpipe.redis.core.proxy;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.core.proxy.parser.compress.CompressAlgorithm;
import com.ctrip.xpipe.redis.core.proxy.parser.path.ProxyPathParser;
import com.ctrip.xpipe.redis.core.proxy.parser.route.ProxyRouteParser;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public class DefaultProxyProtocol implements ProxyProtocol {

    private static final Logger logger = LoggerFactory.getLogger(DefaultProxyProtocol.class);

    private ProxyProtocolParser parser;

    public DefaultProxyProtocol() {

    }

    public DefaultProxyProtocol(ProxyProtocolParser parser) {
        this.parser = parser;
    }

    @Override
    public List<Endpoint> nextEndpoints() {
        ProxyRouteParser routeOptionParser = (ProxyRouteParser) parser.getProxyOptionParser(PROXY_OPTION.ROUTE);
        return routeOptionParser.getNextEndpoints();
    }

    @Override
    public void recordPath(Channel channel) {
        ProxyPathParser pathParser = (ProxyPathParser) parser.getProxyOptionParser(PROXY_OPTION.PATH);
        pathParser.addNodeToPath(channel);
    }

    @Override
    public ByteBuf output() {
        return parser.format();
    }

    @Override
    public ProxyProtocol read(ByteBuf byteBuf) {
        SimpleStringParser simpleString = (SimpleStringParser) new SimpleStringParser().read(byteBuf);
        logger.info("[read] Simple String parse: {}", simpleString.getPayload());
        String protocol = simpleString.getPayload();
        return new DefaultProxyProtocolParser().read(protocol);
    }

    @Override
    public boolean isCompressed() {
        return false;
    }

    @Override
    public CompressAlgorithm compressAlgorithm() {
        return null;
    }
}

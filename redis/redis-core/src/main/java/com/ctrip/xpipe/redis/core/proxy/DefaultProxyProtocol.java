package com.ctrip.xpipe.redis.core.proxy;

import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.parser.compress.CompressAlgorithm;
import com.ctrip.xpipe.redis.core.proxy.parser.path.ProxyForwardForParser;
import com.ctrip.xpipe.redis.core.proxy.parser.route.ProxyRouteParser;
import com.ctrip.xpipe.redis.core.proxy.parser.route.RouteOptionParser;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public class DefaultProxyProtocol implements ProxyProtocol {

    private static final Logger logger = LoggerFactory.getLogger(DefaultProxyProtocol.class);

    private ProxyProtocolParser parser;

    private String content;

    public DefaultProxyProtocol() {

    }

    public DefaultProxyProtocol(ProxyProtocolParser parser) {
        this.parser = parser;
    }

    @Override
    public List<ProxyEndpoint> nextEndpoints() {
        ProxyRouteParser routeOptionParser = (ProxyRouteParser) parser.getProxyOptionParser(PROXY_OPTION.ROUTE);
        return routeOptionParser.getNextEndpoints();
    }

    @Override
    public void recordForwardFor(InetSocketAddress address) {
        ProxyForwardForParser forwardForParser = (ProxyForwardForParser) parser.getProxyOptionParser(PROXY_OPTION.FORWARD_FOR);
        forwardForParser.append(address);
    }

    @Override
    public String getForwardFor() {
        return parser.getProxyOptionParser(PROXY_OPTION.FORWARD_FOR).getPayload();
    }

    @Override
    public ByteBuf output() {
        return parser.format();
    }

    @Override
    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public String getContent() {
        return this.content;
    }

    @Override
    public String getRouteInfo() {
        ProxyRouteParser proxyRouteParser = (ProxyRouteParser) parser.getProxyOptionParser(PROXY_OPTION.ROUTE);
        return proxyRouteParser.getContent();
    }

    @Override
    public String getFinalStation() {
        ProxyRouteParser routeParser = (ProxyRouteParser) parser.getProxyOptionParser(PROXY_OPTION.ROUTE);
        return routeParser.getFinalStation();
    }

    @Override
    public boolean isCompressed() {
        return false;
    }

    @Override
    public CompressAlgorithm compressAlgorithm() {
        return null;
    }

    @Override
    public String toString() {
        return content;
    }
}

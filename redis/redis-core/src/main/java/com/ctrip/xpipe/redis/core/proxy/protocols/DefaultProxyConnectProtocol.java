package com.ctrip.xpipe.redis.core.proxy.protocols;

import com.ctrip.xpipe.api.proxy.CompressAlgorithm;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.ProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser;
import com.ctrip.xpipe.redis.core.proxy.parser.path.ForwardForOptionParser;
import com.ctrip.xpipe.redis.core.proxy.parser.path.ProxyForwardForParser;
import com.ctrip.xpipe.redis.core.proxy.parser.route.ProxyRouteParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public class DefaultProxyConnectProtocol extends AbstractProxyProtocol<ProxyConnectProtocolParser> implements ProxyConnectProtocol {

    private static final Logger logger = LoggerFactory.getLogger(DefaultProxyConnectProtocol.class);

    private static final String UNKNOWN_SOURCE = "UnknownSource";

    private String content;

    public DefaultProxyConnectProtocol(ProxyConnectProtocolParser parser) {
        super(parser);
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
        return parser.getProxyOptionParser(PROXY_OPTION.FORWARD_FOR).output();
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
    public String getSource() {
        ForwardForOptionParser forwardForOptionParser = (ForwardForOptionParser) parser
                .getProxyOptionParser(PROXY_OPTION.FORWARD_FOR);
        String forwardFor = forwardForOptionParser.output();
        if(forwardFor == null || forwardFor.isEmpty()) {
            return UNKNOWN_SOURCE;
        }
        String[] paths = forwardForOptionParser.output().split(AbstractProxyOptionParser.WHITE_SPACE);
        if(paths.length < 2) {
            return UNKNOWN_SOURCE;
        }
        return paths[1];
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

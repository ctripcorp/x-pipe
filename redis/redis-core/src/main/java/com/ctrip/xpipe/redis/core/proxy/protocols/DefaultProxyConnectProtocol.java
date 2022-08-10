package com.ctrip.xpipe.redis.core.proxy.protocols;

import com.ctrip.xpipe.api.proxy.CompressAlgorithm;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.ProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser;
import com.ctrip.xpipe.redis.core.proxy.parser.content.CompressParser;
import com.ctrip.xpipe.redis.core.proxy.parser.content.DefaultProxyContentParser;
import com.ctrip.xpipe.redis.core.proxy.parser.content.ProxyContentParser;
import com.ctrip.xpipe.redis.core.proxy.parser.path.ForwardForOptionParser;
import com.ctrip.xpipe.redis.core.proxy.parser.path.ProxyForwardForParser;
import com.ctrip.xpipe.redis.core.proxy.parser.route.ProxyRouteParser;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public class DefaultProxyConnectProtocol extends AbstractProxyProtocol<ProxyConnectProtocolParser> implements ProxyConnectProtocol {

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
        if(forwardForParser == null) {
            forwardForParser = new ForwardForOptionParser();
            parser.addProxyOptionParser(forwardForParser);
        }
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
        if(forwardForOptionParser == null) {
            return UNKNOWN_SOURCE;
        }
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
        ProxyContentParser contentParser = ((ProxyContentParser)parser.getProxyOptionParser(PROXY_OPTION.CONTENT));
        if(contentParser == null) {
            return false;
        }
        return contentParser.getContentType().equals(ProxyContentParser.ContentType.COMPRESS);
    }

    @Override
    public CompressAlgorithm getCompressAlgorithm() {
        if(isCompressed()) {
            CompressParser compressParser = (CompressParser) ((ProxyContentParser)parser.getProxyOptionParser(PROXY_OPTION.CONTENT)).getSubOption();
            return compressParser.getAlgorithm();
        }
        return null;
    }

    @Override
    public void removeCompressOptionIfExist() {
        DefaultProxyContentParser contentParser = (DefaultProxyContentParser) getParser().getProxyOptionParser(PROXY_OPTION.CONTENT);
        if(contentParser == null) {
            return;
        }
        if(ProxyContentParser.ContentType.COMPRESS.equals(contentParser.getContentType())) {
            getParser().removeProxyOptionParser(contentParser);
        }
    }

    @Override
    public void addCompression(CompressAlgorithm algorithm) {
        removeCompressOptionIfExist();
        DefaultProxyContentParser contentParser = new DefaultProxyContentParser();
        CompressParser compressParser = new CompressParser().setAlgorithm(algorithm);
        contentParser.setType(ProxyContentParser.ContentType.COMPRESS).setSubOptionParser(compressParser);
        getParser().addProxyOptionParser(contentParser);
    }

    @Override
    public boolean isLastHopLeft() {
        ProxyRouteParser routeOptionParser = (ProxyRouteParser) parser.getProxyOptionParser(PROXY_OPTION.ROUTE);
        return routeOptionParser.isLastHopLeft();
    }

    @Override
    public String toString() {
        return content;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultProxyConnectProtocol that = (DefaultProxyConnectProtocol) o;
        return Objects.equals(content, that.content);
    }

    @Override
    public int hashCode() {
        return Objects.hash(content);
    }
    
}

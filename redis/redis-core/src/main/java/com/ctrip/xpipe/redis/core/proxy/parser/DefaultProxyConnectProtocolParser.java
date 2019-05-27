package com.ctrip.xpipe.redis.core.proxy.parser;

import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.redis.core.proxy.ProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.protocols.DefaultProxyConnectProtocol;


/**
 * @author chen.zhu
 * <p>
 * May 04, 2018
 */
@SuppressWarnings("unchecked")
public class DefaultProxyConnectProtocolParser extends AbstractProxyProtocolParser<ProxyConnectProtocol>
        implements ProxyConnectProtocolParser {

    @Override
    protected ProxyConnectProtocol newProxyProtocol(String protocol) {
        ProxyConnectProtocol proxyConnectProtocol = new DefaultProxyConnectProtocol(this);
        proxyConnectProtocol.setContent(protocol);
        return proxyConnectProtocol;
    }

    @Override
    public void addProxyOptionParser(ProxyOptionParser parser) {
        getParsers().add(parser);
    }

    @Override
    public void removeProxyOptionParser(ProxyOptionParser parser) {
        getParsers().remove(parser);
    }
}

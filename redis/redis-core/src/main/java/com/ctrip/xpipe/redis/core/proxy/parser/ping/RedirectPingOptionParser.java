package com.ctrip.xpipe.redis.core.proxy.parser.ping;

import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.command.entity.ProxyPongEntity;
import com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser;
import com.ctrip.xpipe.redis.core.proxy.parser.ProxyReqResOptionParser;

/**
 * @author chen.zhu
 * <p>
 * Nov 01, 2018
 */
public class RedirectPingOptionParser extends AbstractProxyOptionParser implements ProxyReqResOptionParser<ProxyPongEntity> {

    @Override
    public ProxyPongEntity getResponse() {
        return null;
    }

    @Override
    public String output() {
        return null;
    }

    @Override
    public PROXY_OPTION option() {
        return null;
    }
}

package com.ctrip.xpipe.redis.core.proxy.parser.ping;

import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser;
import com.ctrip.xpipe.redis.core.proxy.parser.ProxyOptionParser;
import com.ctrip.xpipe.redis.core.proxy.parser.ProxyReqResOptionParser;


/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2018
 */
public class PingOptionParser extends AbstractProxyOptionParser implements ProxyReqResOptionParser<String> {

    private static final String PING = PROXY_OPTION.PING.name();

    @Override
    public String getContent() {
        return output;
    }

    @Override
    public PROXY_OPTION option() {
        return PROXY_OPTION.PING;
    }

    @Override
    public String output() {
        return PING;
    }

    @Override
    public ProxyOptionParser read(String option) {
        return super.read(option);
    }

}

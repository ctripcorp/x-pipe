package com.ctrip.xpipe.redis.core.proxy.parser;

import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;

/**
 * @author chen.zhu
 * <p>
 * May 04, 2018
 */
public class UnknownOptionParser extends AbstractProxyOptionParser {

    @Override
    public String output() {
        return output;
    }

    @Override
    public ProxyOptionParser read(String option) {
        this.output = option;
        this.originOptionString = option;
        return this;
    }

    @Override
    public PROXY_OPTION option() {
        return PROXY_OPTION.UNKOWN;
    }
}

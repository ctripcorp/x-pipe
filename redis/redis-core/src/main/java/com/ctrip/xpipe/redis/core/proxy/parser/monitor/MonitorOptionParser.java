package com.ctrip.xpipe.redis.core.proxy.parser.monitor;

import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser;
import com.ctrip.xpipe.redis.core.proxy.parser.ProxyReqResOptionParser;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2018
 */
public class MonitorOptionParser extends AbstractProxyOptionParser implements ProxyReqResOptionParser<String> {

    @Override
    public String output() {
        return output;
    }

    @Override
    public PROXY_OPTION option() {
        return PROXY_OPTION.MONITOR;
    }

    @Override
    public String getContent() {
        return output;
    }
}

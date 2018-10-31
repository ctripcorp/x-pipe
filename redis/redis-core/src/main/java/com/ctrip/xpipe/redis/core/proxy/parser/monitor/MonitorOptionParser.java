package com.ctrip.xpipe.redis.core.proxy.parser.monitor;

import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser;
import com.ctrip.xpipe.redis.core.proxy.parser.ProxyReqResOptionParser;
import io.netty.channel.Channel;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2018
 */
public class MonitorOptionParser extends AbstractProxyOptionParser implements ProxyReqResOptionParser<String> {

    @Override
    public String getResponse() {
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

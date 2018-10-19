package com.ctrip.xpipe.redis.core.proxy.parser.monitor;

import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser;
import com.ctrip.xpipe.redis.core.proxy.parser.ProxyProtocolResponse;
import com.ctrip.xpipe.redis.core.proxy.parser.ProxyReqResOptionParser;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2018
 */
public class MonitorOptionParser extends AbstractProxyOptionParser implements ProxyReqResOptionParser {
    @Override
    public ProxyProtocolResponse getResponse() {
        return null;
    }

    @Override
    public PROXY_OPTION option() {
        return null;
    }
}

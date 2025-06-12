package com.ctrip.xpipe.redis.core.proxy.parser.path;

import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser;
import com.ctrip.xpipe.utils.ChannelUtil;

import java.net.InetSocketAddress;

/**
 * @author chen.zhu
 * <p>
 * May 04, 2018
 */
public class ForwardForOptionParser extends AbstractProxyOptionParser implements ProxyForwardForParser {

    @Override
    public void append(InetSocketAddress address) {
        String ipAndPort = ChannelUtil.getSimpleIpport(address);
        if(originOptionString == null || originOptionString.isEmpty()) {
            originOptionString = option().name();
        }
        output = originOptionString + " " + ipAndPort;
    }

    @Override
    public String output() {
        return output;
    }

    @Override
    public PROXY_OPTION option() {
        return PROXY_OPTION.FORWARD_FOR;
    }


}

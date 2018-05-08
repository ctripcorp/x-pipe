package com.ctrip.xpipe.redis.core.proxy.parser.path;

import com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.utils.ChannelUtil;
import io.netty.channel.Channel;

/**
 * @author chen.zhu
 * <p>
 * May 04, 2018
 */
public class PathOptionParser extends AbstractProxyOptionParser implements ProxyPathParser {

    @Override
    public void addNodeToPath(Channel channel) {
        String ipAndPort = ChannelUtil.getSimpleIpport(channel.remoteAddress());
        if(originOptionString == null || originOptionString.isEmpty()) {
            originOptionString = option().name();
        }
        output = originOptionString + " " + ipAndPort;
    }

    @Override
    public PROXY_OPTION option() {
        return PROXY_OPTION.PATH;
    }


}

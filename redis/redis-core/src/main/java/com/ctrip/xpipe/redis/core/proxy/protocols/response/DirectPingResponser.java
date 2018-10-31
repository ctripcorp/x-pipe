package com.ctrip.xpipe.redis.core.proxy.protocols.response;

import com.ctrip.xpipe.api.proxy.ProxyProtocolResponser;
import com.ctrip.xpipe.api.proxy.ProxyRequestResponseProtocol;
import io.netty.channel.Channel;

/**
 * @author chen.zhu
 * <p>
 * Oct 26, 2018
 */
public class DirectPingResponser implements ProxyProtocolResponser {

    @Override
    public void response(Channel channel, ProxyRequestResponseProtocol protocol) {
        protocol.getResponser();
    }

}

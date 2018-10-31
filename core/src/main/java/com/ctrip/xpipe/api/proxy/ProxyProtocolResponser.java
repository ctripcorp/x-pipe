package com.ctrip.xpipe.api.proxy;

import io.netty.channel.Channel;

/**
 * @author chen.zhu
 * <p>
 * Oct 26, 2018
 */
public interface ProxyProtocolResponser {

    void response(Channel channel, ProxyRequestResponseProtocol protocol);
}

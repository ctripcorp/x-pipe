package com.ctrip.xpipe.redis.proxy.listener;

import com.ctrip.xpipe.api.proxy.ProxyRequestResponseProtocol;
import io.netty.channel.Channel;

/**
 * @author chen.zhu
 * <p>
 * Oct 26, 2018
 */
public class DefaultProxyReqResProtocolListener implements ProxyReqResProtocolListener {

    @Override
    public void onCommand(Channel channel, ProxyRequestResponseProtocol protocol) {

    }
}

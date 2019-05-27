package com.ctrip.xpipe.redis.proxy.handler.response;

import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import io.netty.channel.Channel;

public interface ProxyProtocolOptionHandler {

    PROXY_OPTION getOption();

    void handle(Channel channel, String[] content);

}
